# Correctif : aucune réception de blocs pendant la synchronisation

## Causes identifiées (analyse statique)

1. **Réponse de plage rejetée en bloc** — `ClassPeerSyncFunction.CheckPacketBlockDataByRange`
   Le serveur renvoie les blocs de la plage tels quels, y compris les derniers blocs encore `LOCKED`.
   Le client demande `refuseLockedBlock = true`; un seul bloc verrouillé faisait échouer toute la réponse :
   aucun bloc n'était récupéré et le pair honnête recevait un « paquet invalide ».
   -> Les blocs `LOCKED` sont maintenant ignorés individuellement; les autres blocs restent validés strictement
   (un bloc réellement invalide rejette toujours la réponse).

2. **Borne de fin de plage exclusive côté serveur** — `ClassPeerNetworkClientServerObject` (`ASK_BLOCK_DATA_BY_RANGE`)
   Le client traite `BlockHeightEnd` comme inclusif (il attend ce bloc), le serveur s'arrêtait à `End - 1`.
   -> Les deux boucles du serveur utilisent maintenant `<=`.
   Pas de changement de format de paquet; seul le nombre de blocs renvoyés augmente d'un.

3. **Déréférencement avant contrôle de nullité** — `SendAskBlockData` / `SendAskBlockDataByRange`
   `PeerPacketReceived.PacketOrder` était lu avant le test `PeerPacketReceived == null`.
   -> Test de nullité déplacé avant l'accès.

## Non vérifié
Aucune compilation ni test réseau (pas de SDK .NET ni de réseau dans l'environnement d'analyse).
Tests à faire : plage se terminant par des blocs verrouillés, plage de 2 blocs, pair coupé en cours de réponse.
