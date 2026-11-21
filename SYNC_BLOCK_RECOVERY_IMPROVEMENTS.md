# Amélioration de la récupération de synchronisation des blocs

## Changements ciblés
Dans `StartAskBlockObjectFromListPeerTarget` :

- Une réponse invalide pour un bloc isolé ne met plus fin à toute la boucle de récupération du pair : le traitement passe au bloc suivant.
- Une liste de transactions absente ou vide est journalisée et ignorée pour ce bloc.
- Les exceptions sur une requête individuelle sont journalisées avec la hauteur et le pair, puis la récupération continue.
- Les exceptions de la boucle de réception par plage sont journalisées et l'entrée fautive est ignorée.
- `OperationCanceledException` reste propagée afin que l'arrêt demandé ne soit pas transformé en erreur récupérable.

## Compatibilité
Aucun changement au protocole P2P, aux formats de paquets, à la sérialisation des blocs ou aux règles de consensus.

## Validation
Modification source uniquement. La compilation et les tests réseau ne sont pas exécutables dans cet environnement car le SDK .NET/MSBuild n'est pas installé. Construire le projet avec le SDK cible avant déploiement et effectuer des tests de synchronisation longue durée, avec réponses partielles, blocs invalides, pair déconnecté et annulation.
