import functions_framework # obligatoire pour l'exe cloud, a commenter en local


@functions_framework.http # obligatoire pour l'exe cloud, a commenter en local
def ${APPLICATION}(request): # remplacer ${APPLICATION} par le nom de la function 


    return "ok" # Un return est obligatoire pour que l'exécution fonctionne


if __name__ == "__main__":
    ${APPLICATION}(None) # remplacer ${APPLICATION} par le nom de la function 
