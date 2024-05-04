# Use a lightweight base image with Python
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI app code into the container
COPY . .

# Expose the port that the FastAPI app will run on
EXPOSE 8000

# Command to start the FastAPI app using Uvicorn
CMD ["uvicorn", "my_fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"]