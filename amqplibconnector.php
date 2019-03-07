<?php
/**
 * This file contains a PHP client to Celery distributed task queue
 *
 * LICENSE: 2-clause BSD
 *
 * Copyright (c) 2014, GDR!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are those
 * of the authors and should not be interpreted as representing official policies,
 * either expressed or implied, of the FreeBSD Project.
 *
 * @link http://massivescale.net/
 * @link http://gdr.geekhood.net/
 * @link https://github.com/gjedeer/celery-php
 *
 * @package celery-php
 * @license http://opensource.org/licenses/bsd-license.php 2-clause BSD
 * @author GDR! <gdr@go2.pl>
 */

require_once('amqp.php');

use Dtsf\Core\Log;
use Dtsf\Core\WorkerApp;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * Driver for pure PHP implementation of AMQP protocol
 * @link https://github.com/videlalvaro/php-amqplib
 * @package celery-php
 */
class AMQPLibConnector extends AbstractAMQPConnector
{
    /**
     * How long (in seconds) to wait for a message from queue
     * Sadly, this can't be set to zero to achieve complete asynchronity
     */
    public $wait_timeout = 0.1;

    private $connection = null;
    private $connectionDetails = [];
    private $channels = [];
    private $confirmTick = '';
    private $confirmAckTickTime = 5000; //单位s
    private $confirmAckTickTimeRandArr = [4500, 5000, 5500, 6000, 6500];
    private $wokerStopFlag = 1;
    private $waitChan = null;  //该chan为了等待连接池中链接达到最大空闲时间后gc的时候等待最后ack完成在返回true, 否则该链接对象会被直接unset


    /**
     * PhpAmqpLib\Message\AMQPMessage object received from the queue
     */
    private $message = null;

    /**
     * AMQPChannel object cached for subsequent GetMessageBody() calls
     */
    private $receiving_channel = null;

    function GetConnectionObject($details)
    {
        $this->confirmAckTickTime = $this->confirmAckTickTimeRandArr[array_rand($this->confirmAckTickTimeRandArr)];
        $this->connectionDetails = $details;
        $this->channels[$details['exchange']] = new \chan();
        $this->connection = new AMQPConnection(
            $details['host'],
            $details['port'],
            $details['login'],
            $details['password'],
            $details['vhost']
        );

        $this->waitChan = new \chan(1);
        $this->setChannel($details['exchange']);
        return $this->connection;
    }


    /**
     * 创建一个channel
     *
     * @param $exchange
     */
    private function setChannel($exchange)
    {
        if ($this->channels[$exchange]->isEmpty()) {
            $channel = $this->connection->channel();
            $this->channels[$exchange]->push($channel);

            //判断是否开启的ack,return回调
            if (!empty($this->connectionDetails['confirm_ack_callback'])) {
                $channel->set_ack_handler(
                    function (AMQPMessage $message) {
                        if (is_callable($this->connectionDetails['confirm_ack_callback'])) {
                            call_user_func($this->connectionDetails['confirm_ack_callback'], $message);
                        }
                    }
                );
                $channel->set_nack_handler(
                    function (AMQPMessage $message) {
                        if (is_callable($this->connectionDetails['confirm_nack_callback'])) {
                            call_user_func($this->connectionDetails['confirm_nack_callback'], $message);
                        }
                    }
                );
                $channel->confirm_select();
            }

            if (!empty($this->connectionDetails['return_callback'])) {
                $channel->set_return_listener(function ($replyCode, $replyText, $exchange, $routingKey, $message) {
                    if (is_callable($this->connectionDetails['return_callback'])) {
                        call_user_func_array($this->connectionDetails['return_callback'], [
                            $replyCode,
                            $replyText,
                            $exchange,
                            $routingKey,
                            $message
                        ]);
                    }
                });
            }

            if (!empty($this->connectionDetails['return_callback']) ||
                !empty($this->connectionDetails['confirm_ack_callback']) ||
                !empty($this->connectionDetails['confirm_nack_callback'])
            ) {
                //start listen
                $this->listenEvent();
            }
        }
    }

    /**
     * channel 监听
     */
    private function listenEvent()
    {
        $this->confirmTick = swoole_timer_tick($this->confirmAckTickTime, [$this, 'handlerConfirm']);
    }

    /**
     * 处理监听程序,时间回调默认在协程中启动
     */
    public function handlerConfirm()
    {
        $chan = $this->channels[$this->connectionDetails['exchange']]->pop(0.5);
        if ($chan && $chan->getConnection()->isConnected()) {
            $chan->wait_for_pending_acks_returns();
            if ($this->getListenWokerStopFlag() == 3) {
                $chan->close();
                $this->ackFinish();
            } else {
                $this->channels[$this->connectionDetails['exchange']]->push($chan);
            }
        }
    }

    /**
     * worker退出的时候执行最后一次监听,
     */
    public function workerExitHandlerConfirm()
    {
        go(function () {
            $chan = $this->channels[$this->connectionDetails['exchange']]->pop(0.5);
            if ($chan && $this->wokerStopFlag !== 3) {
                $chan->wait_for_pending_acks_returns();
                $this->channels[$this->connectionDetails['exchange']]->push($chan);
                Log::debug("worker {worker_id} execting lask ack on workerExitHandlerConfirm, and current app status is {status}."
                    , ['{worker_id}' => posix_getppid(), '{status}'=>WorkerApp::getInstance()->serverStatus]
                    , WorkerApp::getInstance()->debugDirName);
                //设置停止flag
                $this->wokerStopFlag = 3;
            }
        });

        //如果是进程退出导致的，就不用阻塞等待，因为进程退出检测还有event会频繁请求,
        // 相反如果是链接池资源回收的请求只触发一次workerExitHandlerConfirm，并立刻删除该对象，
        // 所以要阻塞等待最后一次timer回收ack的事件完成在去gc中unset()当前链接对象
        if (WorkerApp::getInstance()->serverStatus !== WorkerApp::WORKEREXIT) {
            Log::debug("worker {worker_id} wait ack finish, and current app status is {status}."
                , ['{worker_id}' => posix_getppid(), '{status}'=>WorkerApp::getInstance()->serverStatus]
                , WorkerApp::getInstance()->debugDirName);
            return $this->waitToStop();
        }
    }

    /**
     * 等待最后ack退出
     * @return mixed
     */
    private function waitToStop()
    {
        return $this->waitChan->pop();
    }

    /**
     * 完成最后ack
     */
    private function ackFinish()
    {
        $this->connection->close();
        //设置worker状态为确认最后ack完成
        WorkerApp::getInstance()->workerSetStatus(4);
        $this->waitChan->push(1);
        swoole_timer_clear($this->confirmTick);
        Log::debug("worker {worker_id} exec last ack at tick, and current app status is {status}.",
            ['{worker_id}' => posix_getppid(), '{status}'=>WorkerApp::getInstance()->serverStatus],
            WorkerApp::getInstance()->debugDirName);
    }

    /**
     * 获取worker停止的flag
     * @return int
     */
    public function getListenWokerStopFlag()
    {
        return $this->wokerStopFlag;
    }

    /* NO-OP: not required in PhpAmqpLib */
    function Connect($connection)
    {
    }

    /**
     * 获取一个channel
     *
     * @param $exchange
     * @param $type
     * @return mixed
     */
    public function getChannel($exchange)
    {
        $channel = $this->channels[$exchange]->pop(0.01);
        if (!is_object($channel)) {
            \Swoole\Coroutine::sleep(0.01);
            return $this->getChannel($exchange);
        }

        return $channel;
    }

    /**
     * 回收channel
     *
     * @param $exchange
     * @param $type
     * @param $channel
     * @return mixed
     */
    public function recycle($exchange, $channel)
    {
        return $this->channels[$exchange]->push($channel);
    }

    /**
     * Return an AMQPTable from a given array
     * @param array $headers Associative array of headers to convert to a table
     */
    private function HeadersToTable($headers)
    {
        return new AMQPTable($headers);
    }

    /**
     * Post a task to exchange specified in $details
     * @param AMQPConnection $connection Connection object
     * @param array $details Array of connection details
     * @param string $task JSON-encoded task
     * @param array $params AMQP message parameters
     */
    function PostToExchange($connection, $details, $task, $params, $headers)
    {
        $ch = $this->getChannel($this->connectionDetails['exchange']);
        $application_headers = $this->HeadersToTable($headers);
        $params['application_headers'] = $application_headers;
        $msg = new AMQPMessage(
            $task,
            $params
        );
        $ch->basic_publish($msg, $details['exchange'], $details['routing_key']);
        $this->recycle($this->connectionDetails['exchange'], $ch);
        return TRUE;
    }

    /**
     * A callback function for AMQPChannel::basic_consume
     * @param PhpAmqpLib\Message\AMQPMessage $msg
     */
    function Consume($msg)
    {
        $this->message = $msg;
    }

    /**
     * Return result of task execution for $task_id
     * @param object $connection AMQPConnection object
     * @param string $task_id Celery task identifier
     * @param int $expire expire time result queue, milliseconds
     * @param boolean $removeMessageFromQueue whether to remove message from queue
     * @return array array('body' => JSON-encoded message body, 'complete_result' => AMQPMessage object)
     *            or false if result not ready yet
     */
    function GetMessageBody($connection, $task_id, $expire = 0, $removeMessageFromQueue = true)
    {
        if (!$this->receiving_channel) {
            $ch = $connection->channel();
            $expire_args = null;
            if (!empty($expire)) {
                $expire_args = array("x-expires" => array("I", $expire));
            }

            $ch->queue_declare(
                $task_id,        /* queue name */
                false,            /* passive */
                true,            /* durable */
                false,            /* exclusive */
                true,            /* auto_delete */
                false,                  /* no wait */
                $expire_args
            );

            $ch->basic_consume(
                $task_id,        /* queue */
                '',            /* consumer tag */
                false,            /* no_local */
                false,            /* no_ack */
                false,            /* exclusive */
                false,            /* nowait */
                array($this, 'Consume')    /* callback */
            );
            $this->receiving_channel = $ch;
        }

        try {
            $this->receiving_channel->wait(null, false, $this->wait_timeout);
        } catch (PhpAmqpLib\Exception\AMQPTimeoutException $e) {
            return false;
        }

        /* Check if the callback function saved something */
        if ($this->message) {
            if ($removeMessageFromQueue) {
                $this->receiving_channel->queue_delete($task_id);
            }
            $this->receiving_channel->close();
            $connection->close();

            return array(
                'complete_result' => $this->message,
                'body' => $this->message->body, // JSON message body
            );
        }

        return false;
    }
}
