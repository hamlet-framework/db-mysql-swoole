<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use Swoole\Coroutine;
use Swoole\Coroutine\MySQL;
use Swoole\Table;
use function gethostbyname;

/**
 * @template-extends Database<MySQL>
 */
class MySQLSwooleDatabase extends Database
{
    /**
     * @var array<string,string>
     */
    private $hosts = [];

    public function __construct(string $host, string $user, string $password, string $databaseName = null, int $poolCapacity = 512)
    {
        $connector = function () use ($host, $user, $password, $databaseName): MySQL {
            $connection = new MySQL();
            if (!isset($this->hosts[$host])) {
                $this->hosts[$host] = gethostbyname($host);
            }
            $params = [
                'host'     => $this->hosts[$host],
                'user'     => $user,
                'password' => $password
            ];
            if ($databaseName) {
                $params['database'] = $databaseName;
            }
            $connection->connect($params);
            return $connection;
        };
        $pool = new MySQLSwooleConnectionPool($connector, $poolCapacity);
        return parent::__construct($pool);
    }

    public function warmUp(int $count)
    {
        $this->pool->warmUp($count);
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

    public static function exception(MySQL $connection): DatabaseException
    {
        return new DatabaseException((string) ($connection->error ?? 'Unknown error'), (int) ($connection->errno ?? -1));
    }
}
