<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\ConnectionPoolInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Swoole\Coroutine\Channel;

class MySQLSwooleConnectionPool implements ConnectionPoolInterface
{
    /**
     * @var callable
     * @psalm-var callable():T
     */
    private $connector;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Channel
     */
    private $pool;

    /**
     * @var int
     */
    private $size;

    /**
     * @param callable $connector
     * @param int $capacity
     * @psalm-param callable():T|false $connector
     */
    public function __construct(callable $connector, int $capacity)
    {
        $this->connector = $connector;
        $this->logger = new NullLogger();
        $this->pool = new Channel($capacity);
        $this->size = $capacity;

        while ($capacity > 0) {
            $db = $connector();
            if ($db !== false) {
                $this->pool->push($db);
                $capacity--;
            }
        }
    }

    /**
     * @param LoggerInterface $logger
     * @return void
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * @return mixed
     * @psalm-return T
     */
    public function pop()
    {
        if ($this->size > 0) {
            $this->logger->debug('Fetching connection from pool (' . count($this->pool) . ' connections left in pool)');
            $this->size--;
            $connection = $this->pool->pop();
        } else {
            $this->logger->debug('Opening new connection');
            $connection = ($this->connector)();
        }
        return $connection;
    }

    /**
     * @param mixed $connection
     * @psalm-param T $connection
     * @return void
     */
    public function push($connection)
    {
        $this->logger->debug('Releasing connection back to pool (' . count($this->pool) . ' connections)');
        $this->size++;
        $this->pool->push($connection);
    }
}
