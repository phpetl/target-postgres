<?php
declare(strict_types=1);

namespace PhpETL\Target\Postgres;

class Config
{
    public readonly string $host;
    public readonly int $port;
    public readonly string $database;
    public readonly string $username;
    public readonly string $password;
    public readonly string $schema;

    static public function fromFile(string $path)
    {
        $json = json_decode(file_get_contents($path), true);

        $instance = new static();
        $instance->host = $json['postgres_host'] ?? 'localhost';
        $instance->port = array_key_exists('postgres_port', $json) ? (int) $json['postgres_port'] : 5432;
        $instance->database = $json['postgres_database'] ?? 'postgres';
        $instance->username = $json['postgres_username'] ?? 'postgres';
        $instance->password = $json['postgres_password'] ?? 'postgres';
        $instance->schema = $json['postgres_schema'] ?? 'postgres';

        return $instance;
    }
}