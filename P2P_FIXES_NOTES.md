# Correctifs P2P – novembre 2026

## Changements

- `TaskManager` accepte désormais des delegates `Func<Task>` et attend leur véritable fin (`Unwrap`), au lieu de traiter des lambdas `async` comme des `Action`/`async void`.
- Les appels `TaskManager.InsertTask(new Action(async ...))` concernés ont été convertis en `Func<Task>` pour que les tâches réseau restent suivies jusqu'à leur terminaison.
- Les tokens fournis à `TaskManager` sont maintenant liés au token global et restent respectés même lorsque `timestampEnd` vaut zéro.
- À l'expiration ou à l'arrêt d'une tâche suivie, son token est annulé avant le nettoyage.
- `ClassCustomSocket.Connect` effectue maintenant la connexion directement avec un timeout (en secondes, comme l'API l'indique) et respecte l'annulation, sans passer par un scheduling différé ambigu.
- Les écritures de paquets P2P côté client sont sérialisées avec un `SemaphoreSlim`, afin d'éviter l'entrelacement des fragments TCP entre requêtes normales et keep-alive.
- La boucle keep-alive utilise un token lié à la connexion et un délai annulable.

## Compatibilité et validation

- Le framing P2P historique Base64 + séparateur est conservé.
- Aucun changement de format de paquet ou de protocole de consensus n'est introduit.
- La compilation n'a pas pu être exécutée dans l'environnement de travail : le SDK `dotnet` n'y est pas installé. Il faut donc compiler et tester sur l'environnement cible avant déploiement.
