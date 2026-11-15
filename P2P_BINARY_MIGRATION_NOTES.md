# Migration du transport P2P vers XENO binaire

## Changements appliqués

- Le client P2P sortant envoie désormais les octets de `GetPacketData()` via `SendBinaryPacketAsync`.
- Le serveur P2P entrant fait de même pour ses réponses.
- Les deux boucles de réception utilisent `ReceiveBinaryPacketAsync` et transmettent directement le `Payload` au parseur de paquets existant.
- Le Base64 d'encapsulation, le séparateur `PacketPeerSplitSeperator` et le découpage textuel ne sont plus utilisés dans ces deux chemins P2P.
- Les écritures sont encadrées par le verrou binaire de `ClassCustomSocket`; le compteur de paquet est local à chaque connexion.
- Le Base64 employé pour la signature de diffusion dans `ClassPeerNetworkBroadcastShortcutFunction` est conservé : il appartient au format du champ de signature, pas au framing TCP.

## Format de trame

Le framing réutilise l'en-tête XENO binaire de 16 octets déjà présent dans `ClassCustomSocket.cs` : magic `XENO`, version, commande, flags, identifiant de paquet et longueur du payload, suivis des octets bruts. Les champs multi-octets sont en Little Endian.

## Compatibilité

Cette modification change le protocole sur le fil. Les anciens nœuds qui parlent Base64 + séparateur ne sont pas compatibles avec les nouveaux nœuds binaires. Il faut mettre à jour simultanément les nœuds P2P d'un réseau, ou ajouter ultérieurement une négociation explicite de version avant un déploiement mixte. Aucun mécanisme de bascule automatique n'a été ajouté.

## Validation

- Recherche statique effectuée : aucun appel `Convert.ToBase64String`, `Convert.FromBase64String`, `TrySendSplittedPacket` ou `TryReadPacketData` ne subsiste dans les deux classes de synchronisation P2P modifiées.
- La signature Base64 de diffusion reste présente intentionnellement.
- La compilation et les tests d'intégration ne sont pas certifiés dans cet environnement : le SDK .NET n'est pas installé. Compiler la solution sur l'environnement cible, puis tester une connexion client/serveur, keep-alive, petits paquets et synchronisation de blocs volumineux avant mise en production.
