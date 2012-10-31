[![Build Status](https://secure.travis-ci.org/unistorage/gridfs-serve.png)](http://travis-ci.org/unistorage/gridfs-serve)

Настройки
=========
Настройки хранятся в settings.py. В секции mongo указываются параметры подключения к монге, в секции app указываются параметры для запуска приложения на встроенном серверею
Пример запуска через uwsgi
==========================
uwsgi --http :9090  --module main --callable gridfs_serve
где main - модуль main.py, содержащий основной код, а gridfs_serve - инстанс класса GridFSServe(самого приложения).
