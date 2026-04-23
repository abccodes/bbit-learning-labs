# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from abc import ABC, abstractmethod

import pika


class mqConsumerInterface(ABC):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic",
            durable=True,
        )
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_name,
            routing_key=self.binding_key,
        )
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message_callback,
            auto_ack=False,
        )

    @abstractmethod
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        """Handle a delivered RabbitMQ message."""

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        if self.channel is None:
            raise RuntimeError("RabbitMQ channel has not been initialized")
        self.channel.start_consuming()

    def close(self) -> None:
        if self.channel is not None and self.channel.is_open:
            self.channel.close()
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    def __del__(self) -> None:
        self.close()
