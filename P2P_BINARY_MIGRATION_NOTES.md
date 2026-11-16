# Migration du transport P2P vers le binaire fragmenté

## Format XENO

- L'en-tête de trame XENO reste fixe à 16 octets : magic `XENO`, version, commande, flags, identifiant de paquet et longueur de charge utile (Little Endian).
- Les envois P2P client et serveur passent par `ClassCustomSocket.TrySendSplittedPacket`.
- Chaque paquet logique est découpé en fragments de données de 64 Kio maximum et chaque fragment est écrit séparément, dans l'ordre.
- Le verrou d'envoi est conservé pendant toute la séquence afin qu'un keep-alive ou un autre paquet ne puisse pas s'intercaler entre deux fragments.
- Le bit `0x8000` du champ flags indique une trame fragmentée. Le payload de chaque trame commence par 16 octets de métadonnées : identifiant logique (uint32), index du fragment (uint32), nombre total de fragments (uint32), longueur totale (uint32), puis les données.
- `ReceiveBinaryPacketAsync` lit exactement chaque trame, vérifie la séquence et les longueurs, puis réassemble le paquet avant de le remettre au parseur P2P.
- La taille maximale d'un paquet logique est configurable avec `ClassCustomSocket.MaximumLogicalPayloadSize` (64 Mio par défaut, plafond de configuration à 256 Mio). Une allocation de réassemblage n'est faite qu'après validation de cette limite.
- Une annulation ou erreur après le début d'une écriture/lecture ferme la connexion pour éviter de continuer sur un flux TCP désynchronisé.

## Encapsulation et compatibilité

- Le transport P2P n'utilise ni Base64 d'encapsulation ni séparateur textuel `*`.
- Les usages Base64 pour signatures, clés, portefeuilles, persistance et protocoles distincts sont conservés.
- Le protocole de fragmentation est incompatible avec les anciens nœuds P2P Base64/`*`. Tous les nœuds d'un même réseau doivent être mis à jour ensemble. Aucune négociation automatique de version n'est implémentée.
- `SendBinaryPacketAsync` reste disponible pour une trame binaire non fragmentée de petite taille ; les chemins de synchronisation P2P doivent utiliser `TrySendSplittedPacket`.

## Validation

- Contrôles statiques effectués : les deux chemins d'envoi P2P appellent `TrySendSplittedPacket`; aucun appel P2P à `SendBinaryPacketAsync` ne subsiste; les limites de trame et de paquet logique sont vérifiées avant allocation.
- La compilation .NET et les tests d'intégration sur sockets réels restent à exécuter dans un environnement avec le SDK .NET installé.
