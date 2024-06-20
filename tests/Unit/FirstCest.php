<?php

/**
 * DB-Batch a DB adapter library handling DB connection, queries, import and export.
 * Copyright (C) 2014  Nordic Genetic Resource Center (NordGen).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    Kjell-Åke Lundblad <kjellake.lundblad@nordgen.org>
 * @copyright 2014- Nordic Genetic Resource Center (NordGen)
 * @license   https://github.com/nordgen/db-batch/blob/master/LICENSE.md AGPL-3.0 Licence
 * @package   nordgen\db-batch
 */


namespace Tests\Unit;

use Laminas\Db\Adapter\Adapter;
use nordgen\DbBatch\DbBatch;
use nordgen\DbBatch\DbBatchInterface;
use nordgen\DbBatch\Models\QueryResult;
use Tests\Support\UnitTester;

class FirstCest
{

    protected ?Adapter $connection = null;

    protected ?DbBatchInterface $dbBatch = null;

    protected array $user = [];
    protected array $profile = [];

    public function _before(UnitTester $I)
    {
        $this->connection = new Adapter(
            [
                'driver'   => 'pdo',
                'dsn'      => 'sqlite:tests/Support/Data/sqlite-database.db',
            ]
        );
        $this->dbBatch = DbBatch::create($this->connection);
    }

    // tests
    public function testIfExpectedTablesExists(UnitTester $I)
    {
        $I->amGoingTo('Test codecept has db connection');
        $I->seeInDatabase('users');
        $I->seeNumRecords(2,'users');
        $I->seeInDatabase('profiles');
        $I->seeNumRecords(2,'profiles');
    }


    public function testMethodQuery(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch query method by first collect user data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table users');
        $this->user[1] = $I->grabEntryFromDatabase('users', ['user_id'=>1]);
        $this->user[2] = $I->grabEntryFromDatabase('users', ['user_id'=>2]);

        $I->comment('Collecting two records of table users by DbBatch.');
        $this->dbBatch->query('SELECT * FROM users WHERE user_id IN (1,2)');
        $rs = $this->dbBatch->getQueryResult();
        if ($rs instanceof QueryResult) {
            /** @var \ArrayObject $r */
            foreach ($rs as $r) {
                $I->assertSame($this->user[$r->offsetGet('user_id')], $r->getArrayCopy());
            }
        }

        $I->amGoingTo('Test DbBatch query method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles');
        $this->profile[1] = $I->grabEntryFromDatabase('profiles', ['profile_id'=>1]);
        $this->profile[2] = $I->grabEntryFromDatabase('profiles', ['profile_id'=>2]);

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $this->dbBatch->query('SELECT * FROM profiles WHERE profile_id IN (1,2)');
        $rs = $this->dbBatch->getQueryResult();
        if ($rs instanceof QueryResult) {
            /** @var \ArrayObject $r */
            foreach ($rs as $r) {
                $I->assertSame($this->profile[$r->offsetGet('profile_id')], $r->getArrayCopy());
            }
        }

    }

    public function testMethodQueryOne(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch queryOne method by first collect user data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table users by DbBatch.');
        $r = $this->dbBatch->queryOne('SELECT * FROM users WHERE user_id IN (1,2)');
        $I->assertSame($this->user[$r['user_id']], $r);

        $I->amGoingTo('Test DbBatch queryOne method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $r = $this->dbBatch->queryOne('SELECT * FROM profiles WHERE profile_id IN (1,2)');
        $I->assertSame($this->profile[$r['profile_id']], $r);
    }

    public function testMethodQueryOneWithParameters(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch queryOne method by first collect user data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table users by DbBatch.');
        $r = $this->dbBatch->queryOne('SELECT * FROM users WHERE user_id = ?', [1]);
        $I->assertSame($this->user[$r['user_id']], $r);

        $I->amGoingTo('Test DbBatch queryOne method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $r = $this->dbBatch->queryOne('SELECT * FROM profiles WHERE profile_id = ?', [1]);
        $I->assertSame($this->profile[$r['profile_id']], $r);
    }

    public function testMethodQueryScalarWithParameters(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch queryScalar method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $column = $this->dbBatch->queryScalar('SELECT last_name FROM profiles WHERE profile_id = ?', [1]);
        //$column = $this->dbBatch->queryScalar('SELECT last_name FROM profiles WHERE profile_id = 1');
        codecept_debug($column);
        $I->assertSame($this->profile[1]['last_name'], $column);
    }


    public function testMethodQueryColumnWithParameters(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch queryScalar method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $column = $this->dbBatch->queryColumn('SELECT last_name FROM profiles WHERE profile_id IN (?,?)',[1,2]);

        codecept_debug($column);
        $I->assertSame([$this->profile[1]['last_name'],$this->profile[2]['last_name']], $column);
    }


    public function testMethodQueryAllWithParameters(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch queryScalar method by first collect profile data by Codeception and then by DbBatch.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $rows = $this->dbBatch->queryAll('SELECT last_name FROM profiles WHERE profile_id IN (?,?)',[1,2]);

        codecept_debug($rows);
        $I->assertSame(array_values(array_map(fn($row) => array_filter($row, fn($column) => $column==='last_name',ARRAY_FILTER_USE_KEY),$this->profile )), $rows);
    }


    public function testParameterizedInsertAndUpdateSqlString(UnitTester $I)
    {
        $I->amGoingTo('Test DbBatch createParameterizedInsertSqlString method.');

        $I->comment('First create the insert sql statement, then calling execute with the statment and parameters.');
        $recordI = [
            'first_name' => 'new first name',
            'last_name' => 'new last name',
            'email' => 'new email',
        ];
        $actualParameterizedInsertSqlString = $this->dbBatch->createParameterizedInsertSqlString('profiles',$recordI);

        $expectedParameterizedInsertSqlString = 'INSERT INTO profiles ("first_name", "last_name", "email") VALUES (:first_name, :last_name, :email)';

        codecept_debug($actualParameterizedInsertSqlString);
        $I->assertEquals($expectedParameterizedInsertSqlString, $actualParameterizedInsertSqlString,'assert that actual creates SQL statement is equal to expected');

        $resultI = $this->dbBatch->execute($actualParameterizedInsertSqlString,$recordI);
        $I->assertIsInt($resultI);
        $I->assertSame(1,$resultI);

        $recordI2 = $this->dbBatch->queryOne('SELECT * FROM profiles WHERE first_name = :first_name',['first_name' => 'new first name']);
        codecept_debug($recordI2);
        $expected = array_merge(['profile_id'=>3],$recordI);
        $I->assertSame($expected,$recordI2);


        $I->amGoingTo('Test DbBatch createParameterizedUpdateSqlString method.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $recordU = [
            'first_name' => 'new first name updated',
            'last_name' => 'new last name updated',
            'email' => 'new email updated',
        ];
        $whereU = [
            'profile_id' => 3,
        ];
        $parametersU = array_merge(array_values($recordU),array_values($whereU));
        $actualParameterizedUpdateSqlString = $this->dbBatch->createParameterizedUpdateSqlString('profiles',$recordU,$whereU);

        $expectedParameterizedUpdateSqlString = 'UPDATE profiles SET "first_name" = :first_name, "last_name" = :last_name, "email" = :email WHERE "profile_id" = :profile_id';

        codecept_debug($actualParameterizedUpdateSqlString);
        $I->assertEquals($expectedParameterizedUpdateSqlString, $actualParameterizedUpdateSqlString);

        $resultU = $this->dbBatch->execute($actualParameterizedUpdateSqlString,$parametersU);
        $I->assertIsInt($resultU);
        $I->assertSame(1,$resultU);

        $recordU2 = $this->dbBatch->queryOne('SELECT * FROM profiles WHERE first_name = :first_name',[':first_name' => 'new first name updated']);
        codecept_debug($recordU2);
        $I->assertSame(array_merge(['profile_id'=>3],$recordU),$recordU2);



        $I->amGoingTo('Test DbBatch createParameterizedUpdateSqlString method with string condition.');

        $I->comment('Collecting two records of table profiles by DbBatch.');
        $recordU = [
            'first_name' => 'new first name updated 2',
            'last_name' => 'new last name updated 2',
            'email' => 'new email updated 2',
        ];
        $whereU = '"profile_id" = 3';
        $parametersU = array_merge(array_values($recordU),array_values($whereU));
        $actualParameterizedUpdateSqlString = $this->dbBatch->createParameterizedUpdateSqlString('profiles',$recordU,$whereU);

        $expectedParameterizedUpdateSqlString = 'UPDATE profiles SET "first_name" = :first_name, "last_name" = :last_name, "email" = :email WHERE "profile_id" = 3';

        codecept_debug($actualParameterizedUpdateSqlString);
        $I->assertEquals($expectedParameterizedUpdateSqlString, $actualParameterizedUpdateSqlString);

        $resultU = $this->dbBatch->execute($actualParameterizedUpdateSqlString,$parametersU);
        $I->assertIsInt($resultU);
        $I->assertSame(1,$resultU);

        $recordU2 = $this->dbBatch->queryOne('SELECT * FROM profiles WHERE first_name = :first_name',[':first_name' => 'new first name updated 2']);
        codecept_debug($recordU2);
        $I->assertSame(array_merge(['profile_id'=>3],$recordU),$recordU2);
    }





}
