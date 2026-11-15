# Migration du transport P2P vers le binaire

## Changements

- Les chemins de synchronisation P2P client et serveur utilisent `SendBinaryPacketAsync` et `ReceiveBinaryPacketAsync`.
- Le framing XENO utilise un en-tête binaire fixe de 16 octets suivi de la charge utile.
- Le Base64 d'encapsulation réseau et le séparateur P2P historique `*` ont été supprimés du code source.
- Les fonctions de réassemblage textuel des paquets P2P (`GetEachPacketSplitted`) et le modèle associé ont été retirés.
- Les appels d'envoi API/RPC qui utilisaient l'ancien nom `TrySendSplittedPacket` utilisent maintenant une écriture brute (`SendRawAsync`). Le protocole HTTP conserve naturellement son terminateur d'en-tête `\r\n\r\n`, qui n'est pas le séparateur P2P `*`.
- Les usages Base64 pour les signatures, clés et bases de données sont conservés : ils ne font pas partie de l'encapsulation du transport P2P.

## Compatibilité

Cette modification change le protocole sur le fil. Les anciens nœuds qui parlent Base64 + séparateur `*` ne sont pas compatibles avec les nouveaux nœuds binaires. Tous les nœuds P2P d'un réseau doivent être mis à jour ensemble, sauf ajout ultérieur d'une négociation explicite de version.

## Vérifications

- Recherche statique du code source : aucune référence à `PacketPeerSplitSeperator`, `TrySendSplittedPacket`, `GetEachPacketSplitted` ou `ClassReadPacketSplitted` ne subsiste.
- La compilation .NET et les tests d'intégration réseau doivent encore être exécutés dans un environnement disposant du SDK requis.
