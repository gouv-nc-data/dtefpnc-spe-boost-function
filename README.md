# Template de fonction


# Exécution
S'assurer que le compte de service dispose des droits nécessaires (ex: ajout en lecture sur fichier Drive)  
S'assurer que les API nécessaires on été activées (via IaC de préférence)
Récupérer les credentials du compte de service et les placer dans le projet.
```
python main.py
```


# Exécution cloud
Dé-commenter les lignes d'exécution cloud.  
Faire un push sur le repo pour lancer le déploiement automatique.  
L'exécution se fera selon le schedule défini dans l'IaC.  
Il est possible de la déclencher manuellement via Workflow ou SchelCloud Scheduler sous GCP.


# Exécution dans un container
## install pack
```
sudo add-apt-repository ppa:cncf-buildpacks/pack-cli
sudo apt-get update
sudo apt-get install pack-cli
```


## construction de l'image
```
pack build  --builder gcr.io/buildpacks/builder:v1 \
  --env GOOGLE_FUNCTION_SIGNATURE_TYPE=http \
  --env GOOGLE_FUNCTION_TARGET=${APPLICATION} \
  --env GOOGLE_PYTHON_VERSION="3.10.x" \
  ${APPLICATION}-function
```

## lancement de l'image en local
```
docker run --rm -p 8080:8080 ${APPLICATION}-function
```

## Appel de la fonction
```
curl localhost:8080
```

# TODO 
- Remplacer tous les ${APPLICATION} par le nom du projet mis dans le module terraform  (https://github.com/gouv-nc-data/data-gitops/blob/main/modules/function_to_bigquery/variables.tf#L6) en replaçant les '-' par des '_'
- Remplacer les TODO

# WARNING
le nom de la fonction appellée est le même que le endpoint