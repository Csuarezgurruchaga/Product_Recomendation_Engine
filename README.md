# Parte Airflow
* Cuando creamos la EC2 tenemos que instalar docker en el ubuntu
* Clonamos el repo
* buildeamos la imagen: sudo docker build -t pa_udesa .
* corremos el container: sudo docker run -p 8000:8000 --name pa_udesa -it pa_udesa /bin/bash
* dentro del container: airflow webserver --port 8000 &
                        airflow scheduler &


# Parte API
* Levantar autoscaling group con launch template ya hecho (esto conecta ECS y levanta una EC2 creando un container de ECR que está linkeado a una task de ECS) -> cada paso tarda, hay que darle tiempo para que levante el autoscaling, la EC2, que se conecte la EC2 a ECS y que dispare la task

* Problema: no corria el service de ECS
    ** Posibles soluciones:
        1) Creamos un IAM rol de tipo ECS, y le sumamos el policy de FullAccess a ECR -> esto se lo pasamos a la task cuando la creamos. La idea sería se falla porque no puede leer el ECR porque es privado
        2) Levantamos un autoscaling que está linkeado a el ECS y, al tener la EC2 activa, la task de ECS puede correr. La idea surge de un error de AWS que decía que no teníamos instancias del container activas dentro de ECS


# Explicación
* Si el cliente no tuvo datos para el día anterior, no va a rener recomendaciones para el día siguiente


