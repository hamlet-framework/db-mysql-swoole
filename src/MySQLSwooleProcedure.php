<?php

namespace Hamlet\Database\MySQLSwoole;

use Generator;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use Swoole\Coroutine\MySQL;

class MySQLSwooleProcedure extends Procedure
{
    use QueryExpanderTrait;

    /** @var MySQL */
    private $handle;

    /** @var string */
    private $query;

    /** @var mixed */
    private $lastInsertId;

    /** @var int|null */
    private $affectedRows;

    public function __construct(MySQL $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    public function execute()
    {
        list($statement) = $this->bindParametersAndExecute($this->handle);
        $this->affectedRows = $statement->affected_rows;
    }

    public function insert(): int
    {
        list($statement) = $this->bindParametersAndExecute($this->handle);
        $this->lastInsertId = $statement->insert_id;
        return $this->lastInsertId;
    }

    /**
     * @return Generator<int,array<string,int|string|float|null>>
     */
    public function fetch(): Generator
    {
        list($statement, $result) = $this->bindParametersAndExecute($this->handle);
        yield from $result;
    }

    public function affectedRows(): int
    {
        return $this->affectedRows ?? -1;
    }

    /**
     * @param MySQL $handle
     * @return array|bool
     */
    private function bindParametersAndExecute(MySQL $handle)
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $key = 'statement_' . md5($query);
        $statement = $this->handle->{$key} ?? $handle->prepare($query);
        if ($statement === false) {
            throw MySQLSwooleDatabase::exception($handle);
        } else {
            $this->handle->{$key} = $statement;
        }

        $values = [];
        foreach ($parameters as list($type, $value)) {
            $values[] = $value;
        }
        return [$statement, $statement->execute($values)];
    }

    public function __destruct()
    {
        $this->handle = null;
    }
}
