<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\{Database, DatabaseException, Session};
use Exception;
use Hamlet\Http\Swoole\Bootstraps\WorkerInitializable;
use Swoole\Coroutine;
use Swoole\Coroutine\{Channel, MySQL};
use function gethostbyname;
use function Hamlet\Cast\_class;

/**
 * @extends Database<MySQL>
 * @psalm-suppress PropertyNotSetInConstructor
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
            /**
             * @psalm-suppress TooManyArguments
             */
            $connection->connect($params);
            return $connection;
        };
        $pool = new MySQLSwooleConnectionPool($connector, $poolCapacity);
        return parent::__construct($pool);
    }

    public function init()
    {
        assert($this->pool instanceof MySQLSwooleConnectionPool);
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

    /**
     * @template K as array-key
     * @template Q
     * @param array<K,callable(Session):Q> $callables
     * @return array<K,Q>
     * @psalm-suppress InvalidReturnStatement
     * @psalm-suppress InvalidReturnType
     * @psalm-suppress MixedArrayAccess
     * @psalm-suppress MixedArrayOffset
     * @psalm-suppress MixedAssignment
     */
    public function withSessions(array $callables): array
    {
        $channel = new Channel(count($callables));
        $result = [];
        foreach ($callables as $key => $callable) {
            /**
             * @psalm-suppress UnusedFunctionCall
             */
            go(function () use ($channel, $callable, $key) {
                $channel->push(
                    $this->withSession(
                        function (Session $session) use ($callable, $key) {
                            return [$key, $callable($session)];
                        }
                    )
                );
            });
            $result[$key] = -1;
        }
        foreach ($callables as $_) {
            list($key, $item) = $channel->pop();
            $result[$key] = $item;
        }
        return $result;
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
