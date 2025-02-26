# /!\ WARNING: RESET EVERYTHING!
# Remove all containers/networks/volumes/images and data in db
docker-compose down
docker system prune -f
docker volume prune -f
docker network prune -f
docker system prune -a -f --volumes              # General cleanup
rm -rf ./mnt/postgres/*
docker stop $(docker ps -q)                      # Stop all containers
docker rm $(docker ps -a -q)                      # Remove all containers
docker rmi -f $(docker images -q)                 # Remove all images (force)

