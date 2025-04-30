FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# Создаем группу supergroup
RUN groupadd supergroup

# Создаем пользователя spark и добавляем его в группу supergroup
RUN useradd -m -g supergroup spark

# Удаляем пароль для пользователя spark (или устанавливаем, если нужно)
RUN passwd -d spark
