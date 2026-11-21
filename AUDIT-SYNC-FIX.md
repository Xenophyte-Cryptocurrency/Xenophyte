# Xenophyte — audit ciblé de la synchronisation P2P

## Symptôme étudié
La hauteur de blockchain cesse de progresser alors que le processus serveur reste actif. Un redémarrage permet à la synchronisation de reprendre.

## Défaut identifié dans le code fourni
Fichier : `Xenophyte-Lib/Instance/Node/Network/Services/P2P/Sync/ClientSync/Service/ClassPeerNetworkSyncServiceObject.cs`

Méthode : `StartAskBlockObjectFromListPeerTarget()`.

La méthode attend que `totalTaskDone` atteigne `totalTaskToDo`. Dans la version fournie, l'incrément de `totalTaskDone` était placé uniquement à la fin du chemin normal du délégué. Une exception survenant avant cette ligne pouvait être interceptée/absorbée par le TaskManager sans incrémenter le compteur. La boucle d'attente restait alors active indéfiniment, bloquant le cycle de synchronisation des blocs alors que le processus restait vivant.

## Correction appliquée
- Encapsulation du traitement de chaque pair dans un `try/catch/finally`.
- Incrément de `totalTaskDone` dans le `finally`, quel que soit le résultat du traitement.
- Propagation de `OperationCanceledException` pour conserver le comportement d'arrêt demandé.
- Journalisation des autres exceptions avec l'identité IP du pair afin de faciliter le diagnostic.
- Aucun changement du protocole P2P, du format des paquets, du stockage des blocs ou des règles de consensus.

## Limites de validation
La correction a été vérifiée par inspection du code et de la structure du projet. La compilation et les tests réseau n'ont pas été exécutés dans cet environnement (SDK .NET non disponible). Il faut compiler avec le SDK correspondant à la cible du projet puis effectuer un test de synchronisation prolongée, avec coupure/reprise d'un pair et réponses P2P invalides ou interrompues.

## Fichier modifié
`Xenophyte-Lib/Instance/Node/Network/Services/P2P/Sync/ClientSync/Service/ClassPeerNetworkSyncServiceObject.cs`
