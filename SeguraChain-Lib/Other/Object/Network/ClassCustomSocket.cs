
using SeguraChain_Lib.Instance.Node.Network.Enum.P2P.Packet;
using SeguraChain_Lib.TaskManager;
using SeguraChain_Lib.Utility;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace SeguraChain_Lib.Other.Object.Network
{
    /// <summary>
    /// Custom TCP socket class.
    /// Supports the XENO binary P2P packet protocol and raw HTTP/API streams.
    /// </summary>
    public class ClassCustomSocket
    {
        private Socket _socket;
        private NetworkStream _networkStream;

        private readonly SemaphoreSlim _binarySendLock =
            new SemaphoreSlim(1, 1);

        private readonly SemaphoreSlim _binaryReceiveLock =
            new SemaphoreSlim(1, 1);

        public bool Closed { private set; get; }

        // ============================================================
        // XENO BINARY PROTOCOL
        // ============================================================

        // Header layout:
        //
        // Offset  Size  Description
        // 0       4     Magic: XENO
        // 4       1     Protocol version
        // 5       1     Command
        // 6       2     Flags
        // 8       4     Packet ID
        // 16      N     Payload
        // 12      4     Data length
        // 16      N     Data
        //
        // All multi-byte fields use Little Endian.

        private const uint BinaryPacketMagic = 0x4F4E4558;
        private const byte BinaryProtocolVersion = 1;
        private const int BinaryHeaderSize = 16;
        public const int BinaryFragmentDataSize =  1024;
        public static int MaximumLogicalDataSize { get; set; } = 64 * 1024 * 1024;
        private const int BinaryFragmentMetadataSize = 16;
        private const int BinaryMaximumFrameDataSize = BinaryFragmentDataSize + BinaryFragmentMetadataSize;
        private const ushort BinaryFragmentFlag = 0x8000;

        /// <summary>
        /// Represents a decoded XENO binary packet.
        /// </summary>
        public sealed class BinaryPacket
        {
            public byte Command { get; set; }

            public ushort Flags { get; set; }

            public uint PacketId { get; set; }

            public byte[] Data { get; set; }
        }

        // ============================================================
        // CONSTRUCTOR
        // ============================================================

        /// <summary>
        /// Constructor.
        /// </summary>
        public ClassCustomSocket(Socket socket, bool isServer)
        {
            _socket = socket;

            TryConfigureTcpSocket(_socket);

            if (isServer)
            {
                try
                {
                    if (_socket != null)
                        _networkStream = new NetworkStream(_socket, true);
                }
                catch
                {
                    Close(SocketShutdown.Both);
                }
            }
        }

        // ============================================================
        // CONNECTION
        // ============================================================

        /// <summary>
        /// Connect with delay (seconds) using TaskManager.
        /// Compatible with .NET Framework 4.8.
        /// </summary>
        public async Task<bool> Connect(
            string ip,
            int port,
            int delay,
            CancellationTokenSource cancellation)
        {
            if (string.IsNullOrWhiteSpace(ip) ||
                port <= 0 ||
                port > 65535 ||
                cancellation == null ||
                cancellation.IsCancellationRequested ||
                _socket == null)
                return false;

            try
            {
                TryConfigureTcpSocket(_socket);

                using (CancellationTokenSource timeout =
                    delay > 0
                        ? new CancellationTokenSource(
                            delay > int.MaxValue / 1000
                                ? int.MaxValue
                                : delay * 1000)
                        : new CancellationTokenSource())
                using (CancellationTokenSource linked =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cancellation.Token,
                        timeout.Token))
                {
                    Task connectTask = _socket.ConnectAsync(ip, port);
                    Task cancellationTask = Task.Delay(
                        Timeout.Infinite,
                        linked.Token);

                    Task completed = await Task.WhenAny(
                        connectTask,
                        cancellationTask);

                    if (completed != connectTask)
                    {
                        SafeKillSocket();
                        return false;
                    }

                    await connectTask;

                    if (linked.IsCancellationRequested ||
                        _socket == null ||
                        !_socket.Connected)
                    {
                        SafeKillSocket();
                        return false;
                    }

                    _networkStream = new NetworkStream(_socket, true);
                    return true;
                }
            }
            catch (Exception error)
            {
                Debug.WriteLine(
                    "Socket connection error to " + ip + ":" +
                    port + " | " + error.Message);

                SafeKillSocket();
                return false;
            }
        }

        // ============================================================
        // SOCKET INFORMATION
        // ============================================================

        public string GetIp
        {
            get
            {
                try
                {
                    var endpoint =
                        _socket?.RemoteEndPoint as IPEndPoint;

                    return endpoint != null
                        ? endpoint.Address.ToString()
                        : string.Empty;
                }
                catch
                {
                    return string.Empty;
                }
            }
        }

        /// <summary>
        /// Checks whether the socket is connected.
        /// </summary>
        public bool IsConnected()
        {
            try
            {
                if (Closed ||
                    _socket == null ||
                    _networkStream == null)
                    return false;

                if (!_socket.Connected)
                    return false;

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Sends raw HTTP/API bytes without application-level delimiters.
        /// </summary>
        public async Task<bool> SendHttpRawAsync(
            byte[] data,
            CancellationToken cancellationToken)
        {
            if (Closed || _networkStream == null || data == null || data.Length == 0)
                return false;

            bool lockTaken = false;
            try
            {
                await _binarySendLock.WaitAsync(cancellationToken);
                lockTaken = true;

                if (Closed || _networkStream == null || cancellationToken.IsCancellationRequested)
                    return false;

                return await _networkStream.SendRawAsync(data, cancellationToken);
            }
            catch (Exception error)
            {
                Debug.WriteLine("Raw send exception to " + GetIp + " | " + error.Message);
                return false;
            }
            finally
            {
                if (lockTaken)
                    _binarySendLock.Release();
            }
        }

        /// <summary>
        /// Sends one logical P2P packet as a sequence of bounded binary frames.
        /// The send lock is held for the complete sequence so keep-alive or
        /// another packet cannot interleave its fragments.
        /// Fragment metadata (16 bytes): logical packet ID, index, count, total length.
        /// </summary>
        public async Task<bool> TrySendSplittedPacket(
            byte command,
            ushort flags,
            uint packetId,
            byte[] data,
            CancellationToken cancellationToken)
        {
            int maximumLogicalDataSize = MaximumLogicalDataSize;
            if (data == null || data.Length == 0 ||
                maximumLogicalDataSize <= 0 ||
                maximumLogicalDataSize > 256 * 1024 * 1024 ||
                data.Length > maximumLogicalDataSize ||
                Closed || _networkStream == null || !IsConnected())
                return false;

            int fragmentCount = (data.Length + BinaryFragmentDataSize - 1) / BinaryFragmentDataSize;
            if (fragmentCount < 1 || fragmentCount > 65535)
                return false;

            bool lockTaken = false;
            bool writeStarted = false;
            try
            {
                await _binarySendLock.WaitAsync(cancellationToken);
                lockTaken = true;

                for (int fragmentIndex = 0; fragmentIndex < fragmentCount; fragmentIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int sourceOffset = fragmentIndex * BinaryFragmentDataSize;
                    int dataLength = System.Math.Min(BinaryFragmentDataSize, data.Length - sourceOffset);
                    byte[] frameData = new byte[BinaryFragmentMetadataSize + dataLength];
                    WriteUInt32LE(frameData, 0, packetId);
                    WriteUInt32LE(frameData, 4, (uint)fragmentIndex);
                    WriteUInt32LE(frameData, 8, (uint)fragmentCount);
                    WriteUInt32LE(frameData, 12, (uint)data.Length);
                    Buffer.BlockCopy(data, sourceOffset, frameData, BinaryFragmentMetadataSize, dataLength);

                    byte[] header = new byte[BinaryHeaderSize];
                    WriteUInt32LE(header, 0, BinaryPacketMagic);
                    header[4] = BinaryProtocolVersion;
                    header[5] = command;
                    WriteUInt16LE(header, 6, (ushort)(flags | BinaryFragmentFlag));
                    WriteUInt32LE(header, 8, packetId);
                    WriteUInt32LE(header, 12, (uint)frameData.Length);

                    writeStarted = true;
                    await _networkStream.WriteAsync(header, 0, header.Length, cancellationToken);
                    await _networkStream.WriteAsync(frameData, 0, frameData.Length, cancellationToken);
                }
                return true;
            }
            catch (OperationCanceledException)
            {
                if (writeStarted) Close(SocketShutdown.Both);
                return false;
            }
            catch (Exception error)
            {
#if DEBUG
                Debug.WriteLine("XENO fragmented send error to " + GetIp + " | " + error.Message);
#endif
                if (writeStarted) Close(SocketShutdown.Both);
                return false;
            }
            finally
            {
                if (lockTaken) _binarySendLock.Release();
            }
        }

        // ============================================================
        // XENO BINARY PACKET RECEIVING
        // ============================================================

        /// <summary>
        /// Receives and validates one complete XENO binary packet.
        ///
        /// Returns null if:
        /// - The connection is closed.
        /// - The packet header is invalid.
        /// - The protocol version is unsupported.
        /// - The Data exceeds the maximum size.
        /// - The operation times out or is cancelled.
        /// - An I/O error occurs.
        ///
        /// If a frame has started but cannot be completed,
        /// the connection is closed to prevent stream desynchronization.
        /// </summary>
        public async Task<BinaryPacket> ReceiveBinaryPacketAsync(
            int timeoutMilliseconds,
            CancellationToken cancellationToken)
        {
            if (Closed || _networkStream == null)
                return null;

            bool lockTaken = false;
            bool readingStarted = false;
            using (CancellationTokenSource timeout = timeoutMilliseconds > 0
                ? new CancellationTokenSource(timeoutMilliseconds)
                : new CancellationTokenSource())
            using (CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, timeout.Token))
            {
                try
                {
                    await _binaryReceiveLock.WaitAsync(linked.Token);
                    lockTaken = true;
                    if (Closed || _networkStream == null) return null;

                    BinaryPacket first = await ReadBinaryFrameNoLock(linked.Token, () => readingStarted = true);
                    if (first == null) return null;

                    if ((first.Flags & BinaryFragmentFlag) == 0)
                        return first;

                    if (first.Data == null || first.Data.Length < BinaryFragmentMetadataSize)
                        throw new InvalidDataException("Invalid XENO fragment metadata.");

                    uint logicalPacketId = ReadUInt32LE(first.Data, 0);
                    uint fragmentCount = ReadUInt32LE(first.Data, 8);
                    uint totalLength = ReadUInt32LE(first.Data, 12);
                    int maximumLogicalDataSize = MaximumLogicalDataSize;
                    if (logicalPacketId != first.PacketId || fragmentCount < 1 || fragmentCount > 65535 ||
                        maximumLogicalDataSize <= 0 ||
                        maximumLogicalDataSize > 256 * 1024 * 1024 ||
                        totalLength == 0 || totalLength > maximumLogicalDataSize ||
                        ReadUInt32LE(first.Data, 4) != 0)
                        throw new InvalidDataException("Invalid XENO first fragment.");

                    int expectedCount = ((int)totalLength + BinaryFragmentDataSize - 1) / BinaryFragmentDataSize;
                    if (fragmentCount != expectedCount)
                        throw new InvalidDataException("Inconsistent XENO fragment count.");

                    byte[] assembled = new byte[(int)totalLength];
                    int firstDataLength = first.Data.Length - BinaryFragmentMetadataSize;
                    int expectedFirstDataLength = System.Math.Min(BinaryFragmentDataSize, assembled.Length);
                    if (firstDataLength != expectedFirstDataLength)
                        throw new InvalidDataException("Invalid XENO first fragment length.");
                    Buffer.BlockCopy(first.Data, BinaryFragmentMetadataSize, assembled, 0, firstDataLength);
                    int assembledLength = firstDataLength;

                    for (uint index = 1; index < fragmentCount; index++)
                    {
                        BinaryPacket next = await ReadBinaryFrameNoLock(linked.Token, () => readingStarted = true);
                        if (next == null || next.Command != first.Command || next.PacketId != first.PacketId ||
                            (next.Flags & BinaryFragmentFlag) == 0 ||
                            (next.Flags & ~BinaryFragmentFlag) != (first.Flags & ~BinaryFragmentFlag) ||
                            next.Data == null || next.Data.Length < BinaryFragmentMetadataSize ||
                            ReadUInt32LE(next.Data, 0) != logicalPacketId ||
                            ReadUInt32LE(next.Data, 4) != index ||
                            ReadUInt32LE(next.Data, 8) != fragmentCount ||
                            ReadUInt32LE(next.Data, 12) != totalLength)
                            throw new InvalidDataException("Out-of-order or inconsistent XENO fragment.");

                        int dataLength = next.Data.Length - BinaryFragmentMetadataSize;
                        int expectedDataLength = System.Math.Min(BinaryFragmentDataSize, assembled.Length - assembledLength);
                        if (dataLength != expectedDataLength || dataLength <= 0)
                            throw new InvalidDataException("Invalid XENO fragment Data length.");
                        Buffer.BlockCopy(next.Data, BinaryFragmentMetadataSize, assembled, assembledLength, dataLength);
                        assembledLength += dataLength;
                    }

                    if (assembledLength != assembled.Length)
                        throw new InvalidDataException("Incomplete XENO logical packet.");

                    return new BinaryPacket
                    {
                        Command = first.Command,
                        Flags = (ushort)(first.Flags & ~BinaryFragmentFlag),
                        PacketId = first.PacketId,
                        Data = assembled
                    };
                }
                catch (OperationCanceledException)
                {
                    if (readingStarted) Close(SocketShutdown.Both);
                    return null;
                }
                catch (Exception error)
                {
                    Debug.WriteLine("XENO binary receive error from " + GetIp + " | " + error.Message);
                    if (readingStarted) Close(SocketShutdown.Both);
                    return null;
                }
                finally
                {
                    if (lockTaken) _binaryReceiveLock.Release();
                }
            }
        }

        private async Task<BinaryPacket> ReadBinaryFrameNoLock(
            CancellationToken cancellationToken,
            Action onReadStarted)
        {
            byte[] header = new byte[BinaryHeaderSize];
            onReadStarted?.Invoke();
            await ReadExactlyAsync(header, cancellationToken);

            if (ReadUInt32LE(header, 0) != BinaryPacketMagic)
                throw new InvalidDataException("Invalid XENO packet magic.");
            if (header[4] != BinaryProtocolVersion)
                throw new InvalidDataException("Unsupported XENO protocol version.");

            uint DataLength = ReadUInt32LE(header, 12);
            if (DataLength > BinaryMaximumFrameDataSize)
                throw new InvalidDataException("XENO frame exceeds maximum allowed size.");

            byte[] Data = new byte[(int)DataLength];
            if (Data.Length > 0)
                await ReadExactlyAsync(Data, cancellationToken);

            return new BinaryPacket
            {
                Command = header[5],
                Flags = ReadUInt16LE(header, 6),
                PacketId = ReadUInt32LE(header, 8),
                Data = Data
            };
        }

        // ============================================================
        // BINARY STREAM READING
        // ============================================================

        /// <summary>
        /// Reads exactly the requested number of bytes.
        /// Does not filter, trim or modify binary data.
        /// </summary>
        private async Task ReadExactlyAsync(
            byte[] buffer,
            CancellationToken cancellationToken)
        {
            int offset = 0;

            while (offset < buffer.Length)
            {
                int received = await _networkStream.ReadAsync(
                    buffer,
                    offset,
                    buffer.Length - offset,
                    cancellationToken);

                if (received == 0)
                {
                    throw new EndOfStreamException(
                        "TCP connection closed during XENO packet reception.");
                }

                offset += received;
            }
        }

        // ============================================================
        // LITTLE ENDIAN CONVERSION
        // ============================================================

        private static void WriteUInt16LE(
            byte[] buffer,
            int offset,
            ushort value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
        }

        private static void WriteUInt32LE(
            byte[] buffer,
            int offset,
            uint value)
        {
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        private static ushort ReadUInt16LE(
            byte[] buffer,
            int offset)
        {
            return (ushort)(
                buffer[offset] |
                (buffer[offset + 1] << 8));
        }

        private static uint ReadUInt32LE(
            byte[] buffer,
            int offset)
        {
            return
                (uint)buffer[offset] |
                ((uint)buffer[offset + 1] << 8) |
                ((uint)buffer[offset + 2] << 16) |
                ((uint)buffer[offset + 3] << 24);
        }
  
        // ============================================================
        // RAW HTTP/API STREAM RECEIVING
        // ============================================================

        /// <summary>
        /// Receives a legacy packet.
        /// Legacy Base64 filtering is preserved.
        /// </summary>
        public async Task<ReadPacketData> TryReadPacketHttpData(
            int packetLength,
            int delayReading,
            bool isHttp,
            CancellationTokenSource cancellation)
        {
            var readPacketData = new ReadPacketData
            {
                Status = false,
                Data = packetLength > 0
                    ? new byte[packetLength]
                    : Array.Empty<byte>()
            };

            if (Closed ||
                _networkStream == null ||
                packetLength <= 0 ||
                cancellation == null ||
                cancellation.IsCancellationRequested)
                return readPacketData;

            CancellationTokenSource ctsDelay = null;
            CancellationTokenSource ctsLinked = null;

            int offset = 0;

            try
            {
                ctsDelay = delayReading > 0
                    ? new CancellationTokenSource(delayReading)
                    : new CancellationTokenSource();

                ctsLinked =
                    CancellationTokenSource.CreateLinkedTokenSource(
                        cancellation.Token,
                        ctsDelay.Token);

                while (offset < packetLength)
                {
                    int read = await _networkStream.ReadAsync(
                        readPacketData.Data,
                        offset,
                        packetLength - offset,
                        ctsLinked.Token);

                    if (read <= 0)
                        break;

                    offset += read;

                    if (isHttp && offset >= 4)
                    {
                        if (ContainsHttpHeaderTerminator(
                            readPacketData.Data,
                            offset))
                            break;
                    }
                }

                // Use the number of bytes actually read.
                // Do not infer the length by trimming trailing zero bytes.
                if (offset != readPacketData.Data.Length)
                {
                    byte[] resized = new byte[offset];

                    if (offset > 0)
                    {
                        Buffer.BlockCopy(
                            readPacketData.Data,
                            0,
                            resized,
                            0,
                            offset);
                    }

                    readPacketData.Data = resized;
                }
            }
            catch (Exception error)
            {
#if DEBUG
                Debug.WriteLine(
                    "Reading legacy packet exception from " +
                    GetIp + " | " + error.Message);
#endif
            }
            finally
            {
                try { ctsLinked?.Dispose(); } catch { }
                try { ctsDelay?.Dispose(); } catch { }
            }

            readPacketData.Status =
                readPacketData.Data != null &&
                readPacketData.Data.Length > 0;

            return readPacketData;
        }

        // ============================================================
        // HTTP STREAM HELPERS
        // ============================================================

        private static bool ContainsHttpHeaderTerminator(
            byte[] buffer,
            int length)
        {
            for (int i = 0; i <= length - 4; i++)
            {
                if (buffer[i] == (byte)'\r' &&
                    buffer[i + 1] == (byte)'\n' &&
                    buffer[i + 2] == (byte)'\r' &&
                    buffer[i + 3] == (byte)'\n')
                    return true;
            }

            return false;
        }

        // ============================================================
        // SOCKET CLOSING
        // ============================================================

        public void Kill(SocketShutdown shutdownType)
        {
            Close(shutdownType);
        }

        private void Close(SocketShutdown shutdownType)
        {
            if (Closed)
                return;

            Closed = true;

            try
            {
                try
                {
                    _networkStream?.Close();
                }
                catch { }

                try
                {
                    _networkStream?.Dispose();
                }
                catch { }

                _networkStream = null;

                if (_socket != null)
                {
                    try
                    {
                        if (_socket.Connected)
                            _socket.Shutdown(shutdownType);
                    }
                    catch { }

                    try
                    {
                        _socket.Close();
                    }
                    catch { }

                    try
                    {
                        _socket.Dispose();
                    }
                    catch { }

                    _socket = null;
                }
            }
            catch
            {
                // Ignored.
            }
        }

        private void SafeKillSocket()
        {
            try
            {
                if (_socket != null)
                {
                    try { _socket.Close(); } catch { }
                    try { _socket.Dispose(); } catch { }
                }
            }
            catch
            {
                // Ignored.
            }
            finally
            {
                _socket = null;
                _networkStream = null;
                Closed = true;
            }
        }

        // ============================================================
        // TCP CONFIGURATION
        // ============================================================

        /// <summary>
        /// Centralizes TCP socket options.
        /// Compatible with .NET Framework 4.8.
        /// </summary>
        private static void TryConfigureTcpSocket(Socket socket)
        {
            if (socket == null)
                return;

            try
            {
                socket.NoDelay = true;

                if (socket.SendBufferSize < 256 * 1024)
                    socket.SendBufferSize = 256 * 1024;

                if (socket.ReceiveBufferSize < 256 * 1024)
                    socket.ReceiveBufferSize = 256 * 1024;

                socket.SetSocketOption(
                    SocketOptionLevel.Socket,
                    SocketOptionName.KeepAlive,
                    true);

                socket.LingerState =
                    new LingerOption(true, 0);
            }
            catch
            {
                // Ignored.
            }
        }

        // ============================================================
        // LEGACY READ RESULT
        // ============================================================

        public class ReadPacketData : IDisposable
        {
            public bool Status;

            public byte[] Data;

            private bool _disposed;

            public void Dispose()
            {
                Dispose(true);
            }

            private void Dispose(bool dispose)
            {
                if (_disposed || !dispose)
                    return;

                Data = null;
                Status = false;
                _disposed = true;
            }
        }
    }
}
