<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\{Database, DatabaseException, Session};
use Exception;
use Hamlet\Http\Swoole\Bootstraps\WorkerInitializable;
use Swoole\Coroutine;
use Swoole\Coroutine\MySQL;
use function gethostbyname;

/**
 * @template-extends Database<MySQL>
 */
class MySQLSwooleDatabase extends Database implements WorkerInitializable
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
                'host'       => $this->hosts[$host],
                'user'       => $user,
                'password'   => $password
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

    public function init()
    {
        $this->pool->init();
    }

    public function withSession(callable $callable)
    {
        $handle = $this->pool->pop();
        Coroutine::defer(function () use ($handle) {
            $this->pool->push($handle);
        });
        $session = $this->createSession($handle);
        try {
            return $callable($session);
        } catch (DatabaseException $e) {
            throw $e;
        } catch (Exception $e) {
            throw new DatabaseException('Failed to execute statement', 0, $e);
        }
    }

    protected function createSession($handle): Session
    {
        $session = new MySQLSwooleSession($handle);
        $session->setLogger($this->logger);
        return $session;
    }

    public static function exception(MySQL $connection): DatabaseException
    {
        return new DatabaseException((string) ($connection->error ?? 'Unknown error'), (int) ($connection->errno ?? -1));
    }
}
