
<table>
  <tr>
    <td style="border: none; padding-top: 8px;">
      <a href="https://boosty.to">
        <img src=".github/Dark.png" width="36" height="36" alt="Boosty" />
      </a>
    </td>
    <td style="border: none; padding-left: 8px; vertical-align: middle;">
      <a href="https://boosty.to">
        <b>Поддержать разработку проекта на Boosty</b>
      </a>
    </td>
  </tr>
</table>

# LinkReduction

Link Reduction App — 

Это приложение для сокращения ссылок, которое позволяет пользователям создавать короткие ссылки из длинных URL и переходить по ним.  
Приложение поддерживает HTTP-запросы и интеграцию с Telegram-ботом для удобного сокращения ссылок.

## Использование через HTTP-запрос

- curl -X POST https://linkreduction.mooo.com:8443/api/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "http://example.com"}'

- Ответ: строка с короткой ссылкой, например:  
https://linkreduction.mooo.com:8443/dcdfb4

## Использование через telegram-bot

- бот доступен по ссылке https://t.me/linkreduction_bot
- Просто передайте ему необходимую ссылку которую хотите сократить
- Он вернёт вам соращенную

## Переход по короткой ссылке

- Откройте в браузере: https://linkreduction.mooo.com:8443/dcdfb4

- Перенаправление на оригинальный URL (например, http://example.com).

### Ссылка будет существовать 2 недели

## Быстрый старт

### Основные команды

- **Запуск приложения с пересборкой образа**  
  make up
- **Остановка и удаление контейнеров (без сохранения состояния)**  
  make down
- **Остановка контейнеров (с возможностью повторного запуска)**  
  make stop
- **Сборка Docker образа приложения вручную**  
  make build
- **Инициализация модуля Go (go.mod)**  
  make gomod

## Установка TDM-GCC (на Windows) для вызова команды покрытия тестами

  - Перейди на сайт:  
    👉 https://jmeubank.github.io/tdm-gcc/  
  - Скачай и установи версию TDM-GCC 64-bit  
  - Во время установки убедиcь, что стоит галочка Add to PATH
  - После установки перезапусти терминал и проверьте:  
    gcc --version

## Использование Config

- В проекте используется viper для работы с конфигурациями

- Вы можете создать свой config.yaml на основе internal/config/config.example.yaml

## Основные технологии проекта

- Postgres
- Redis
- Kafka
- Prometheus
- Grafana