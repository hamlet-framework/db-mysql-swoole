<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\ConnectionPoolInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\MySQL;

/**
 * @implements ConnectionPoolInterface<MySQL>
 */
class MySQLSwooleConnectionPool implements ConnectionPoolInterface
{
    /**
     * @var callable
     * @psalm-var callable():MySQL
     */
    private $connector;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Channel
     */
    private $channel;

    /**
     * @param callable $connector
     * @param int $capacity
     * @psalm-param callable():MySQL|false $connector
     */
    public function __construct(callable $connector, int $capacity)
    {
        $this->connector = $connector;
        $this->logger    = new NullLogger();
        $this->channel   = new Channel($capacity);
    }

    public function warmUp(int $count)
    {
        while ($count > 0) {
            $db = ($this->connector)();
            if ($db !== false) {
                $this->channel->push($db);
                $count--;
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
     * @psalm-return MySQL
     */
    public function pop()
    {
        $connection = $this->channel->pop();
        if ($connection === false) {
            $this->logger->debug('Opening new connection');
            $connection = ($this->connector)();
        }
        return $connection;
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    public function push($connection)
    {
        $success = $this->channel->push($connection);
        if (!$success) {
            $this->logger->debug("Cannot return connection to pool, closing");
            $connection->close();
        }
    }
}
