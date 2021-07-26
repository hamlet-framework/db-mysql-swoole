<?php

namespace Hamlet\Database\MySQLSwoole;

use Generator;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use Swoole\Coroutine\MySQL;
use Swoole\Coroutine\MySQL\Statement;
use function Hamlet\Cast\_float;
use function Hamlet\Cast\_int;
use function Hamlet\Cast\_map;
use function Hamlet\Cast\_null;
use function Hamlet\Cast\_string;
use function Hamlet\Cast\_union;

class MySQLSwooleProcedure extends Procedure
{
    use QueryExpanderTrait;

    /**
     * @var MySQL
     */
    private $handle;

    /**
     * @var string
     */
    private $query;

    /**
     * @var int|null
     */
    private $lastInsertId = null;

    /**
     * @var int|null
     */
    private $affectedRows = null;

    public function __construct(MySQL $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    public function execute()
    {
        list($statement) = $this->bindParametersAndExecute($this->handle);
        $this->affectedRows = _int()->assert($statement->affected_rows);
    }

    public function insert(): int
    {
        list($statement) = $this->bindParametersAndExecute($this->handle);
        $this->lastInsertId = _int()->assert($statement->insert_id);
        return $this->lastInsertId;
    }

    /**
     * @return Generator<int,array<string,int|string|float|null>>
     */
    public function fetch(): Generator
    {
        list($_, $result) = $this->bindParametersAndExecute($this->handle);
        assert(($type = _map(_int(), _map(_string(), _union(_int(), _string(), _null(), _float())))) && $type->matches($result), var_export($result, true));
        yield from $result;
    }

    public function affectedRows(): int
    {
        return $this->affectedRows ?? -1;
    }

    /**
     * @param MySQL $handle
     * @return array{Statement,mixed}
     * @psalm-suppress LessSpecificReturnStatement
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MoreSpecificReturnType
     */
    private function bindParametersAndExecute(MySQL $handle): array
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $key = 'statement_' . md5($query);
        $statement = property_exists($this->handle, $key) ? $this->handle->{$key} : false;
        if (!$statement) {
            $statement = $handle->prepare($query);
            if ($statement) {
                $this->handle->{$key} = $statement;
            } else {
                throw new DatabaseException(sprintf('Cannot prepare statement %s', $query));
            }
        }
        assert($statement instanceof Statement);

        $values = [];
        foreach ($parameters as list($_, $value)) {
            $values[] = $value;
        }
        $result = $statement->execute($values);
        return [$statement, $result];
    }

    public function __destruct()
    {
        $this->handle = null;
    }
}
