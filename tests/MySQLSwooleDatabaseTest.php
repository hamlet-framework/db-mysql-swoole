<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\Database;
use Hamlet\Database\Procedure;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MySQLSwooleDatabaseTest extends TestCase
{
    /** @var int */
    private $userId;

    private function connect(): Database
    {
        return new MySQLSwooleDatabase('0.0.0.0', 'root', '123456', 'test');
    }

    private function fetch(): Procedure
    {
        $database = $this->connect();
        return $database->prepare('
            SELECT users.id,
                   name,
                   address
              FROM users 
                   JOIN addresses
                     ON users.id = addresses.user_id      
        ');
    }

    public function setUp()
    {
        $database = $this->connect();

        $procedure = $database->prepare("INSERT INTO users (name) VALUES ('Vladimir')");
        $this->userId = $procedure->insert();

        $procedure = $database->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Moskva')");
        $procedure->bindInteger($this->userId);
        $procedure->execute();

        $procedure = $database->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Vladivostok')");
        $procedure->bindInteger($this->userId);
        $procedure->execute();
    }

    public function tearDown()
    {
        $database = $this->connect();

        $database->prepare('DELETE FROM addresses WHERE 1')->execute();
        $database->prepare('DELETE FROM users WHERE 1')->execute();
    }

    public function testEverything()
    {
        $result = $this->fetch()->processOne()
            ->coalesceAll()
            ->collectAll();

        Assert::assertEquals([$this->userId], $result);
    }



    public function testProcessAll()
    {
        $result = $this->fetch()->processAll()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->collectAll();

        Assert::assertCount(1, $result);
        Assert::assertArrayHasKey($this->userId, $result);
        Assert::assertEquals('Vladimir', $result[$this->userId]['name']);
        Assert::assertCount(2, $result[$this->userId]['addresses']);
    }

    public function testFetchOne()
    {
        Assert::assertEquals(['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'], $this->fetch()->fetchOne());
    }

    public function testFetchAll()
    {
        Assert::assertEquals([
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'],
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Vladivostok']
        ], $this->fetch()->fetchAll());
    }

    public function testStream()
    {
        $iterator = $this->fetch()->stream()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->iterator();

        foreach ($iterator as $id => $user) {
            Assert::assertEquals($this->userId, $id);
            Assert::assertCount(2, $user['addresses']);
        }
    }

    public function testInsert()
    {
        $database = $this->connect();

        $procedure = $database->prepare("INSERT INTO users (name) VALUES ('Anatoly')");
        Assert::assertGreaterThan($this->userId, $procedure->insert());
    }
}
