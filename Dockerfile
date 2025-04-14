# Usar la imagen base oficial de Playwright para Python
# FROM mcr.microsoft.com/playwright:v1.51.0-noble
# FROM mcr.microsoft.com/playwright/python:focal
FROM mcr.microsoft.com/playwright/python:v1.51.0-noble

# Instalar Xvfb y dependencias necesarias
#RUN apt-get update && \
#    apt-get install -y xvfb && \
#    rm -rf /var/lib/apt/lists/*

# Crear un usuario no root
# RUN useradd -m pwuser

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos de tu proyecto al contenedor
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

# Cambiar el propietario de los archivos al usuario no root
RUN chown -R pwuser:pwuser /app

# Cambiar al usuario no root
USER pwuser

# RUN pip install --no-cache-dir --upgrade pip setuptools wheel
# RUN pip install playwright
# RUN /home/pwuser/.local/bin/playwright install chromium

# Comando por defecto (puedes cambiarlo según tus necesidades)
CMD ["python", "download.py"]