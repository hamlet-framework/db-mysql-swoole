<?php

namespace Hamlet\Database\MySQLSwoole;

use Exception;
use Hamlet\Database\ConnectionPool;
use Hamlet\Http\Swoole\Bootstraps\WorkerInitializable;
use Psr\Log\{LoggerInterface, NullLogger};
use Swoole\Coroutine\{Channel, MySQL};

/**
 * @implements ConnectionPool<MySQL>
 */
class MySQLSwooleConnectionPool implements ConnectionPool, WorkerInitializable
{
    /**
     * @var callable
     * @psalm-var callable():MySQL|false
     */
    private $connector;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Channel|null
     */
    private $pool;

    /**
     * @var int
     */
    private $capacity;

    /**
     * @param callable $connector
     * @param int $capacity
     * @psalm-param callable():MySQL|false $connector
     */
    public function __construct(callable $connector, int $capacity)
    {
        $this->connector = $connector;
        $this->logger    = new NullLogger;
        $this->capacity  = $capacity;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function init()
    {
        $this->pool = new Channel($this->capacity);
        $count = $this->capacity;
        while ($count > 0) {
            try {
                $connection = ($this->connector)();
                if ($connection !== false) {
                    $this->pool->push($connection);
                    $count--;
                }
            } catch (Exception $e) {
                $this->logger->warning('Failed to establish connection', ['exception' => $e]);
            }
        }
    }

    /**
     * @return MySQL
     */
    public function pop()
    {
        while (true) {
            $connection = $this->pool->pop();
            if ($connection !== false) {
                return $connection;
            }
            sleep(0.01);
        }
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    public function push($connection)
    {
        $this->pool->push($connection);
    }
}
