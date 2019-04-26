<?php

namespace Hamlet\Database\MySQLSwoole;

use co;
use Hamlet\Database\ConnectionPool;
use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use Swoole\Coroutine\MySQL;

/**
 * @template-extends Database<T>
 */
class MySQLSwooleDatabase extends Database
{
    /**
     * @var array<string,string>
     */
    static $hosts = [];

    public function __construct(string $host, string $user, string $password, string $databaseName = null)
    {
        $connector = function () use ($host, $user, $password, $databaseName): MySQL {
            $connection = new MySQL();
            if (!isset(self::$hosts[$host])) {
                self::$hosts[$host] = co::gethostbyname($host);
            }
            $params = [
                'host'     => self::$hosts[$host],
                'user'     => $user,
                'password' => $password
            ];
            if ($databaseName) {
                $params['database'] = $databaseName;
            }
            $attempts = 1000;
            while ($attempts-- > 0) {
                $connection->connect($params);
                if ($connection) {
                    break;
                }
                co::sleep(0.0001);
            }
            return $connection;
        };
        $pool = new ConnectionPool($connector);
        return parent::__construct($pool);
    }

    public function prepare(string $query): Procedure
    {
        $procedure = new MySQLSwooleProcedure($this->executor(), $query);
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
        $success = $connection->begin();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    protected function commit($connection)
    {
        $this->logger->debug('Committing transaction');
        $success = $connection->commit();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    protected function rollback($connection)
    {
        $this->logger->debug('Rolling back transaction');
        $success = $connection->rollback();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    public static function exception(MySQL $connection): DatabaseException
    {
        return new DatabaseException((string) ($connection->error ?? 'Unknown error'), (int) ($connection->errno ?? -1));
    }
}
