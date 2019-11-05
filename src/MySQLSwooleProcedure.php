<?php

namespace Hamlet\Database\MySQLSwoole;

use Generator;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use Swoole\Coroutine\MySQL;

class MySQLSwooleProcedure extends Procedure
{
    use QueryExpanderTrait;

    /** @var callable */
    private $executor;

    /** @var string */
    private $query;

    /** @var mixed */
    private $lastInsertId;

    /** @var int|null */
    private $affectedRows;

    public function __construct(callable $executor, string $query)
    {
        $this->executor = $executor;
        $this->query = $query;
    }

    public function execute()
    {
        ($this->executor)(function (MySQL $connection) {
            $this->bindParametersAndExecute($connection);
            $this->affectedRows = $connection->affected_rows;
        });
    }

    public function insert(): int
    {
        return ($this->executor)(function (MySQL $connection) {
            $this->bindParametersAndExecute($connection);
            $this->lastInsertId = $connection->insert_id;
            return $this->lastInsertId;
        });
    }

    /**
     * @return Generator<int,array<string,int|string|float|null>>
     */
    public function fetch(): Generator
    {
        $data = ($this->executor)(function (MySQL $connection) {
            return $this->bindParametersAndExecute($connection);
        });
        yield from $data;
    }

    public function affectedRows(): int
    {
        return $this->affectedRows ?? -1;
    }

    /**
     * @param MySQL $connection
     * @return array|bool
     */
    private function bindParametersAndExecute(MySQL $connection)
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $key = 'statement_' . md5($query);
        $statement = $connection->{$key} ?? $connection->{$key} = $connection->prepare($query);
        if ($statement === false) {
            throw MySQLSwooleDatabase::exception($connection);
        }

        $values = [];
        foreach ($parameters as list ($type, $value)) {
            $values[] = $value;
        }
        return $statement->execute($values);
    }
}
