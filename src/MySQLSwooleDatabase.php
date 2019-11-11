<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\{Database, DatabaseException, Session};
use Swoole\Coroutine\MySQL;
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
