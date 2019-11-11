<?php

namespace Hamlet\Database\MySQLSwoole;

use Hamlet\Database\Database;
use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MySQLSwooleDatabaseTest extends TestCase
{
    /** @var Database */
    private $database;

    /** @var Procedure */
    private $procedure;

    /** @var int */
    private $userId;

    public function setUp()
    {
        $this->database = new MySQLSwooleDatabase('0.0.0.0', 'root', '', 'test');

        $this->database->withSession(function (Session $session) {
            $procedure = $session->prepare("INSERT INTO users (name) VALUES ('Vladimir')");
            $this->userId = $procedure->insert();

            $procedure = $session->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Moskva')");
            $procedure->bindInteger($this->userId);
            $procedure->execute();

            $procedure = $session->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Vladivostok')");
            $procedure->bindInteger($this->userId);
            $procedure->execute();

            $this->procedure = $session->prepare('
                SELECT users.id,
                       name,
                       address
                  FROM users 
                       JOIN addresses
                         ON users.id = addresses.user_id      
            ');
        });
    }

    public function tearDown()
    {
        $this->database->withSession(function (Session $session) {
            $session->prepare('DELETE FROM addresses WHERE 1')->execute();
            $session->prepare('DELETE FROM users WHERE 1')->execute();
        });
    }

    public function testProcessOne()
    {
        $result = $this->procedure->processOne()
            ->coalesceAll()
            ->collectAll();

        Assert::assertEquals([$this->userId], $result);
    }

    public function testProcessAll()
    {
        $result = $this->procedure->processAll()
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
        Assert::assertEquals(['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'], $this->procedure->fetchOne());
    }

    public function testFetchAll()
    {
        Assert::assertEquals([
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'],
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Vladivostok']
        ], $this->procedure->fetchAll());
    }

    public function testStream()
    {
        $iterator = $this->procedure->stream()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->iterator();

        foreach ($iterator as $id => $user) {
            Assert::assertEquals($this->userId, $id);
            Assert::assertEquals(['Moskva', 'Vladivostok'], $user['addresses']);
        }
    }

    public function testInsert()
    {
        $this->database->withSession(function (Session $session) {
            $procedure = $session->prepare("INSERT INTO users (name) VALUES ('Anatoly')");
            Assert::assertGreaterThan($this->userId, $procedure->insert());
        });
    }

    public function testUpdate()
    {
        $this->database->withSession(function (Session $session) {
            $procedure = $session->prepare("UPDATE users SET name = 'Vasily' WHERE name = 'Vladimir'");
            $procedure->execute();
            Assert::assertEquals(1, $procedure->affectedRows());

            $procedure = $session->prepare("UPDATE users SET name = 'Nikolay' WHERE name = 'Evgeniy'");
            $procedure->execute();
            Assert::assertEquals(0, $procedure->affectedRows());
        });
    }
}
