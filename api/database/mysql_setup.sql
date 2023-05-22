-- Comandos para la creación de la base de datos


CREATE DATABASE challenge;                          -- Crea la base de datos
CEATE USER 'user'@'localhost';                      -- Crea usuario que se utiliza para el conector con Python y la ejecución de queries
grant all on challenge.* to "user"@'localhost';     -- Le da permisos universales al usuario para fines del ejercicio.