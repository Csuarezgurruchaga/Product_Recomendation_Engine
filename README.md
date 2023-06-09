## Proyect Diagram
![Proyect Diagram](https://github.com/Csuarezgurruchaga/Product_Recomendation_Engine/blob/main/Proyect%20Diagram.png)

## Airflow Part

1. When we create the EC2, we need to install Docker on Ubuntu.
2. Clone the repo.
3. Build the image: `sudo docker build -t pa_udesa .`
4. Run the container: `sudo docker run -p 8000:8000 --name pa_udesa -it pa_udesa /bin/bash`
5. Inside the container:
   - Start the Airflow webserver: `airflow webserver --port 8000 &`
   - Start the Airflow scheduler: `airflow scheduler &`

## API Part

1. Launch the autoscaling group with a pre-made launch template (this connects ECS and spins up an EC2 by creating a container from ECR linked to an ECS task). Each step takes time, so we need to give it time to set up the autoscaling, EC2, establish the connection between EC2 and ECS, and trigger the task.




