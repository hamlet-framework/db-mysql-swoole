<?php

namespace Hamlet\Database\MySQLSwoole;

use DomainException;
use Exception;
use Hamlet\Database\ConnectionPool;
use Hamlet\Http\Swoole\Bootstraps\WorkerInitializable;
use Psr\Log\{LoggerInterface, NullLogger};
use Swoole\Coroutine\{Channel, MySQL};
use function Hamlet\Cast\_class;

/**
 * @implements ConnectionPool<MySQL>
 */
class MySQLSwooleConnectionPool implements ConnectionPool, WorkerInitializable
{
    /**
     * @var callable():(MySQL|false)
     */
    private $connector;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Channel|null
     */
    private $pool = null;

    /**
     * @var int
     */
    private $capacity;

    /**
     * @param callable():(MySQL|false) $connector $connector
     * @param int $capacity
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
        $i = $this->capacity;
        while ($i > 0) {
            try {
                $connection = ($this->connector)();
                if ($connection !== false) {
                    $this->pool->push($connection);
                    $i--;
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
        if ($this->pool === null) {
            throw new DomainException('Pool not initialized');
        }
        return _class(MySQL::class)->assert($this->pool->pop());
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    public function push($connection)
    {
        if ($this->pool === null) {
            throw new DomainException('Pool not initialized');
        }
        $this->pool->push($connection);
    }
}
