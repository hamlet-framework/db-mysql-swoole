<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\ConnectionPool;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use SplFixedArray;
use Swoole\Atomic;
use Swoole\Coroutine;
use Swoole\Coroutine\MySQL;

/**
 * @implements ConnectionPool<MySQL>
 */
class MySQLSwooleConnectionPool implements ConnectionPool
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
     * @var SplFixedArray
     * @psalm-var SplFixedArray<MySQL>
     */
    private $pool;

    /**
     * @var Atomic
     */
    private $size;

    /**
     * @param callable $connector
     * @param int $capacity
     * @psalm-param callable():MySQL|false $connector
     */
    public function __construct(callable $connector, int $capacity)
    {
        $this->connector = $connector;
        $this->logger    = new NullLogger;
        $this->pool      = new SplFixedArray($capacity);
        $this->size      = new Atomic(0);
    }

    public function warmUp(int $count)
    {
        while ($count > 0) {
            $db = ($this->connector)();
            if ($db !== false) {
                $this->pool[$this->size->add(1)] = $db;
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
        while ($this->size->get() == 0) {
            Coroutine::sleep(0.0001);
        }
        return $this->pool[$this->size->sub(1)];
    }

    /**
     * @param MySQL $connection
     * @return void
     */
    public function push($connection)
    {
        $this->pool[$this->size->add(1)] = $connection;
    }
}
