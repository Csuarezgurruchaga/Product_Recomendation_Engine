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

2. Problem: The ECS service was not running.
   - **Possible solutions:**
     - Create an ECS IAM role and add the FullAccess policy to ECR. Pass this role to the task when creating it. The idea is that it may fail because it cannot read the private ECR.
     - Set up an autoscaling group linked to ECS. With the EC2 instance active, the ECS task can run. This idea arose from an AWS error stating that there were no active container instances within ECS.
