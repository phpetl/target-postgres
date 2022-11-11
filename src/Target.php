<?php
declare(strict_types=1);

namespace PhpETL\Target\Postgres;

use Aura\SqlQuery\QueryFactory;

class Target
{
    protected \PDO $pdo;
    protected array $currentSchema = [];

    public function __construct(protected Config $config)
    {
        $this->pdo = new \PDO(
            sprintf(
                'pgsql:host=%s;port=%d;dbname=%s;user=%s;password=%s',
                $this->config->host,
                $this->config->port,
                $this->config->database,
                $this->config->username,
                $this->config->password,
            )
        );
    }

    public function createNewTable($schema)
    {
        $sql = 'CREATE TABLE ' . $schema['tap_stream_id'] . '(' . PHP_EOL;
        foreach ($schema['schema']['properties'] as $columnName => $properties) {
            
            if (in_array($properties['type'], ['int', 'integer']) && in_array($columnName, $schema['key_properties'])) {
                $sql .= '"' . $columnName . '" SERIAL';
            } else {
                $sql .= '"' . $columnName . '" ' . $this->buildColumnType($properties);
            }
            $sql .= ',' . PHP_EOL;
        }
        $sql .= 'PRIMARY KEY(' . $schema['key_properties'][0] . '));';

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute();
    }

    protected function buildColumnType($properties): string
    {
        $definition = '';
        $nullable = false;

        if (is_array($properties['type'])) {
            $nullKey = array_search("null", $properties['type']);
            if ($nullKey !== false) {
                $nullable = true;
                unset($properties['type'][$nullKey]);
            }

            // We allow multiple data types, so distill it down to a varchar
            if (count($properties['type']) === 1) {
                $properties['type'] = array_pop($properties['type']);
            } else {
                $properties['type'] = 'string';
                if (!array_key_exists('maxLength', $properties)) {
                    $properties['maxLength'] = 255;
                }
            }
        }

        switch($properties['type']) {
            case 'bool':
            case 'boolean':
                $definition = 'boolean';
                break;
            case 'int':
            case 'integer':
            case 'float':
            case 'number':
                if ($properties['type'] === 'integer' || $properties['type'] === 'int') {
                    $definition .= 'integer';
                    break;
                }

                $definition .= 'decimal';
                break;
            case 'string':
                if (array_key_exists('maxLength', $properties)) {
                    if ($properties['maxLength'] < 256) {
                        $definition .= 'varchar(' . $properties['maxLength'] . ')';
                        break;
                    } else {
                        $definition .= 'text';
                        break;
                    }

                }

                if (array_key_exists('format', $properties)) {
                    if ($properties['format'] === 'date-time') {
                        $definition .= 'timestamp';
                    }

                    if ($properties['format'] === 'date') {
                        $definition .= 'date';
                    }

                    if ($properties['format'] === 'time') {
                        $definition .= 'time';
                    }

                    if ($properties['format'] === 'uid') {
                        $definition = 'varchar(255)';    
                    }
                }

                if ($definition === '') {
                    $definition = 'text';
                }
                break;
            case 'object':
                $definition .= 'jsonb';
                break;
            default:
                throw new \RuntimeException('Unknown casting type of ' . $properties['type']);
                break;
        }

        if ($nullable === false) {
            $definition .= ' NOT NULL';
        }

        return $definition;
    }

    public function processRecord(array $record)
    {
        if (empty($this->currentSchema)) {
            throw new \RuntimeException('A schema must be defined before a record can be processed');
        }

        if ($record['type'] === 'RECORD') {
            $factory = new QueryFactory('pgsql');
            $keys = array_keys($record['record']);
            $insert = $factory->newInsert();
            $insert
                ->into($this->currentSchema['tap_stream_id'])
            ->cols($keys)
                ->bindValues($record['record']);
            $stmt = $this->pdo->prepare($insert->getStatement());

            try {
                $stmt->execute($insert->getBindValues());
            } catch (\PDOException $e) {
                if ($e->getCode() === "23505") {
                    $update = $factory->newUpdate();
                    $update
                        ->table($this->currentSchema['tap_stream_id'])
                        ->cols(array_keys($record['record']))
                        ->where($this->currentSchema['key_properties'][0] . ' = :' . $this->currentSchema['key_properties'][0])
                        ->bindValues($record['record']);
                    $stmt = $this->pdo->prepare($update->getStatement());
                    $stmt->execute($update->getBindValues());
                } else {
                    var_dump($e, $insert->getStatement(), $insert->getBindValues());
                    die();
                }
            }
            
            return;
        }

        throw new \InvalidArgumentException('Invalid record definition');
    }

    public function setSchema(array $schema): static
    {
        if ($schema['type'] === 'SCHEMA') {
            $this->currentSchema = $schema;
            $sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema LIKE 'public' AND table_type LIKE 'BASE TABLE' AND table_name = '" . $schema['tap_stream_id'] . "');";
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute();

            if ($stmt->fetch(\PDO::FETCH_ASSOC)['exists'] === false) {
                $this->createNewTable($schema);
            }

            return $this;
        }

        throw new \InvalidArgumentException('Invalid schema definition');
    }
}