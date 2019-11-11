<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\{Procedure, Session};
use Swoole\Coroutine\MySQL;

/**
 * @extends Session<MySQL>
 */
class MySQLSwooleSession extends Session
{
    /**
     * @param MySQL $handle
     */
    public function __construct(MySQL $handle)
    {
        parent::__construct($handle);
    }

    /**
     * @param string $query
     * @return Procedure
     */
    public function prepare(string $query): Procedure
    {
        $procedure = new MySQLSwooleProcedure($this->handle, $query);
        $procedure->setLogger($this->logger);
        return $procedure;
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    protected function startTransaction($connection)
    {
        $this->logger->debug('Starting transaction');
        $connection->begin();
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    protected function commit($connection)
    {
        $this->logger->debug('Committing transaction');
        $connection->commit();
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    protected function rollback($connection)
    {
        $this->logger->debug('Rolling back transaction');
        $connection->rollback();
    }
}
