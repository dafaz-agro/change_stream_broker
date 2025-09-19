# Change Stream Broker

Welcome 👋

**Change Stream Broker** é um pacote Node.js que transforma o MongoDB Change Streams em um sistema completo de message broker para arquiteturas de microsserviços. Ele fornece uma API similar ao Kafka/RabbitMQ, mas utilizando apenas MongoDB como backbone, reduzindo drasticamente a complexidade de implementação e administração, bem como os custos de infraestrutura e processamento.

**Development CLI** torna a experiência de desenvolmento intuitiva, mantém a configuração do message broker e o schema do payload das mensagens no mesmo local do código. As alterações feitas são publicadas via linha de comando.

## Instalação

```bash
npm install @dafaz/change-stream-broker
```

## Inicialização

### 1. Crie os arquivos de Configuração e de Schema do broker:

```bash
npx csbroker init
```

### 2. Edite os arquivos:

Encontre a pasta (change-stream) na raíz do seu projeto:

```text
[root]/
└── change-stream/
        ├── config.ts
        └── message-payload.schema.ts/
```

No arquivo de configurações você terá um serviço de message broker pré-configurado, para você customizar
conforme sua necessidade, e ainda, um exemplo de configuração de um producer e de um consumer.

No arquivo de schema de mensagens, você terá um exemplo de payload de messagem pré-configurado. Edite o arquivo conforme a necessidade do seu microsserviço. 

É importante observar que tanto o arquivo de configuração quanto o de schema, devem permanecer na pasta change-stream gerada na raíz de seu projeto.


### 3. Após a edição, gere os arquivos internos a serem utilizados pelo broker:

```bash
npx csbroker generate
```

Observação: Sempre que editar os arquivos de configuração e de schema, chame o generate para enviar as alterações para o broker.

### 4. Variáveis de ambiente:

No arquivo .env da sua aplicação, crie as seguintes variáveis de conexão com sua instância do MongoDB:

```text

# MongoDB
MONGODB_BROKER_URI="mongodb://root:docker@127.0.0.1:27017/?replicaSet=rs0&authSource=admin"
MONGODB_BROKER_DATABASE="purchase-events"

```

### Demais comandos da CLI

#### Modo Watch:

Utilize o modo watch, em desenvolvimento, para auto-gerar os arquivos do broker, sempre que for realizada uma alteração nos arquivos de configuração ou schema:

```bash
nox csbroker watch
```


#### Backups:

A cada generate, os arquivos antigos são colocados em um backup, e caso seja necessário, podem ser recuperados. Para visualizar a lista de backups, utilize o comando:

```bash
npx csbroker backups
```

#### Restore:

Para recuperar um backup, utilize o comando:

```bash
npx csbroker restore [backup_name]
```

**Atenção** 
Sempre que fizer um restore, você precisa verificar os arquivos de configuração e schema específicos desse restore. Esses arquivos ficam guardados em um Stage. Os comandos à seguir, podem ser utilizados para você comparar os seus arquivos de configuração e schema atuais, com os arquivos em stage. Faça a atualização dos seus arquivos de configuração e schema conforme houver necessidade.

#### Stage:

Os arquivos de geração utilizados pelo broker, ficam em stage, e podem ser verificados através do comando abaixo:

```bash
npx csbroker stage
```

#### Apply Stage:

Após o restore, e a verificação dos arquivos em stage, utilize o comando a seguir para atualizar seus arquivos de configuração e schema em desenvolvimento:

```bash
npx csbroker apply-stage
```


#### Diferenças entre Stages:

Caso você queira uma forma mais eficiente de verificar as diferenças entre o estágio atual do broker e seus arquivos de configuração e schema, utilize o comando:

```bash
npx csbroker diff
```
  

## Principais Características

- 🎯 **API Familiar**: Interface similar a brokers populares (Kafka-like).
- 📦 **Zero Dependências Extras**: Usa apenas o driver oficial do MongoDB.
- 🔄 **Resume Tokens**: Garante delivery exactly-once com mecanismo de offsets.
- 👥 **Consumer Groups**: Suporte a grupos de consumidores com balanceamento automático.
- 🛡️ **Type Safety**: Completo suporte a TypeScript com generics.
- ⚡ **Alta Disponibilidade**: Reconexão automática e lógica de retries integrada.
- 🔧 **Extensível**: Arquitetura modular para customizações.

## Casos de Uso Ideais

- Microsserviços que já utilizam MongoDB.
- Sistemas que precisam de comunicação assíncrona entre serviços.
- Migração de sistemas legados para arquitetura orientada a eventos.
- Ambientes onde Kafka ou RabbitMQ seriam excessivos (overkill).


## Pré-requisitos

- Node.js 16+  
- MongoDB 5.0+ com replica set habilitado
- TypeScript **(recomendado)**


### Configuração do MongoDB

- Para usar **Change Stream Broker** no seu projeto, é necessário configurar o MongoDB em replica set.
- Ou ainda, utilize o **MongoDB Atlas Online**, que já vem com replica set por padrão habilitado.

#### Exemplo de Configuração do MongoDB replica set em um container Docker

O Change Stream exige que a sua instância do MongoDB esteja em um cluster.
Em desenvolvimento, você pode simular esse cluster. Para isso, será necessário criar o docker-compose e o Dockerfile da seguinte forma:

[root] / docker-compose.yml

```yml
version: '3.8'
 
services:
  mongo:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: mongo-server
    restart: always
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: docker
    ports:
      - "0.0.0.0:27017:27017"
    expose:
      - 27017
    command: --replSet rs0 --keyFile /etc/mongo-keyfile --bind_ip_all --port 27017 --auth
    healthcheck:
      test: echo "try { rs.status() } catch (err) { rs.initiate({_id:'rs0',members:[{_id:0,host:'127.0.0.1:27017'}]}) }" | mongosh --port 27017 -u root -p docker --authenticationDatabase admin
      interval: 5s
      timeout: 15s
      start_period: 15s
      retries: 10
    volumes:
      - "./data:/data/db"
      - "./data/config:/etc/mongod.conf"
    networks:
      - mongo-net
  
networks:
  mongo-net:
    driver: bridge
```

[root-path] / Dockerfile

```text
FROM mongo
RUN openssl rand -base64 756 > /etc/mongo-keyfile 
RUN chmod 400 /etc/mongo-keyfile 
RUN chown mongodb:mongodb /etc/mongo-keyfile 
```

Inicialize o container docker:

```bash
docker compose up -d
```


## Exemplo de Desenvolvimento de um Producer em NestJS:

### Purchases

Instale o broker 

```bash
npm install @dafaz/change-stream-broker
```

#### Edite o arquivo .env da aplicação:

```
MONGODB_BROKER_URI="mongodb://root:docker@127.0.0.1:27017/?replicaSet=rs0&authSource=admin"
MONGODB_BROKER_DATABASE="purchase-events"
```

#### Crie os arquivos de configuração e schema do broker

```bash
npx csbroker init
```

#### [root] / change-stream / config

```text
// ==============================================
// EXAMPLE - BROKER CONFIGURATION
// ==============================================
export const brokerConfig = defineBroker({
	mongoUri: process.env.MONGODB_BROKER_URI,
	database: process.env.MONGODB_BROKER_DATABASE,
	autoCreateTopics: true,
	logLevel: 'INFO',
	logContext: 'Purchase Service Broker',
})

// ==============================================
// EXAMPLE - CREATE PRODUCERS
// ==============================================

export const purchasesProducerConfig = defineProducer({
	topic: 'purchases.new-purchase',
	partitions: 1,
	retentionMs: 7 * 24 * 60 * 60 * 1000,
	partitionStrategy: 'hash',
})
```


#### [root] / change-stream / message-payload.schema.ts

```text
// ==============================================
// EXAMPLE - MESSAGE PAYLOAD SCHEMAS
// ==============================================

export interface PurchaseCreatedPayload {
	customer: {
		id: string
		authUserId: string
	}
	product: {
		id: string
		title: string
		slug: string
	}
}
```

#### Geração dos arquivos para o broker:

```bash
npx csbroker generate
```

### Desenvolvimento

#### src / messaging / change-stream.service.ts

```javascript
import { ChangeStreamBroker } from '@dafaz/change-stream-broker'
import { brokerConfig } from '@dafaz/change-stream-broker/client'
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common'

@Injectable()
export class ChangeStreamBrokerService
	extends ChangeStreamBroker
	implements OnModuleInit, OnModuleDestroy
{
	constructor() {
		super(brokerConfig)
	}

	async onModuleInit() {
		await this.connect()
	}
	async onModuleDestroy() {
		await this.disconnect()
	}
}
```

#### src / messaging / messaging.module.ts

```javascript
import { Module } from '@nestjs/common'
import { ChangeStreamBrokerService } from './change-stream.service'

@Module({
	providers: [ChangeStreamBrokerService],
	exports: [ChangeStreamBrokerService],
})
export class MessagingModule {}
```

Importe MessagingModule no módulo principal, ou no módulo cuja as classes dependem de ChangeStreamBrokerService.


#### src / services / purchases.service.ts

```javascript
[...]

  const docToSend: PurchaseCreatedPayload = {
        customer: {
          id: customer.id,
          authUserId: customer.authUserId,
        },
        product: {
          id: product.id,
          title: product.title,
          slug: product.slug,
        },
      }

  const producer = await this.changeStreamBroker.createProducer(
    purchasesProducerConfig,
  )

  await producer.send({
    key: purchase.id,
    value: docToSend, // message-payload
    timestamp: new Date(),
    headers: {
      eventType: 'purchase.created',
      source: 'purchases-service',
    },
  })

[...]
```

## Exemplo de desenvolvimento do Consumer em NestJS:

Instale o broker 

```bash
npm install @dafaz/change-stream-broker
```

#### Edite o arquivo .env da aplicação:

```
MONGODB_BROKER_URI="mongodb://root:docker@127.0.0.1:27017/?replicaSet=rs0&authSource=admin"
MONGODB_BROKER_DATABASE="purchase-events"
```

#### Crie os arquivos de configuração e schema do broker

```bash
npx csbroker init
```

#### [root] / change-stream / config

```text
// ==============================================
// EXAMPLE - BROKER CONFIGURATION
// ==============================================
export const brokerConfig = defineBroker({
	mongoUri: process.env.MONGODB_BROKER_URI,
	database: process.env.MONGODB_BROKER_DATABASE,
	autoCreateTopics: true,
	logLevel: 'INFO',
	logContext: 'Purchase Service Broker',
})

// ==============================================
// EXAMPLE - CREATE CONSUMERS
// ==============================================

export const purchasesConsumerConfig = defineConsumer({
	groupId: 'classroom-service',
	topic: 'purchases.new-purchase',
	partitions: [0],
	autoCommit: true,
	autoCommitIntervalMs: 15000,
	fromBeginning: false,
	maxRetries: 3,
	retryDelayMs: 1000,
	enableOffsetMonitoring: false,
	options: {
		batchSize: 100,
		maxAwaitTimeMS: 1000,
		fullDocument: 'updateLookup',
	},
})
```


#### [root] / change-stream / message-payload.schema.ts

```text
// ==============================================
// EXAMPLE - MESSAGE PAYLOAD SCHEMAS
// ==============================================

export interface PurchaseCreatedPayload {
	customer: {
		id: string
		authUserId: string
	}
	product: {
		id: string
		title: string
		slug: string
	}
}
```

#### Geração dos arquivos para o broker:

```bash
npx csbroker generate
```

### Desenvolvimento

#### src / messaging / change-stream.service.ts


```javascript
import {
	ChangeStreamBroker,
	ChangeStreamConsumer,
	ConsumerRecord,
	MessageHandlerConfig,
} from '@dafaz/change-stream-broker'
import {
	brokerConfig,
	PurchaseCreatedPayload,
	purchasesConsumerConfig,
} from '@dafaz/change-stream-broker/client'
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common'
import { EnrollmentsService } from '../http/services/enrollments.service'

@Injectable()
export class ChangeStreamBrokerService
	extends ChangeStreamBroker
	implements OnModuleInit, OnModuleDestroy
{
	private consumer: ChangeStreamConsumer

	constructor(private readonly enrollmentsService: EnrollmentsService) {
		super(brokerConfig)
	}

	async onModuleInit() {
		await this.connect()

		const handlerConfig: MessageHandlerConfig<PurchaseCreatedPayload> = {
			handler: this.handlePurchaseEvent.bind(this),
			errorHandler: this.handleError.bind(this),
			maxRetries: 3,
			retryDelay: 1000,
			autoCommit: true,
		}

		this.consumer = await this.createConsumer(purchasesConsumerConfig)
		await this.consumer.subscribe(handlerConfig)

		console.log('Enrollment consumer started listening for purchase events...')
	}
	async onModuleDestroy() {
		if (this.consumer) {
			await this.consumer.unsubscribe()
		}

		await this.disconnect()
	}

	async handlePurchaseEvent(payload: ConsumerRecord<PurchaseCreatedPayload>) {
		const purchase = payload.message.value

		try {
			await this.enrollmentsService.createEnrollment(purchase)
		} catch (error) {
			console.error(`Error creating enrollment: ${error.message}`)
			throw error
		}
	}

	private async handleError(error: Error, record?: any) {
		console.error('Error processing purchase event:', error)

		if (record) {
			console.error('Failed record:', record.message.value)
		}
	}
}
```

#### src / messaging / messaging.module.ts

```javascript
import { Module } from '@nestjs/common'
import { DatabaseModule } from '../database/database.module'
import { EnrollmentsService } from '../http/services/enrollments.service'
import { ChangeStreamBrokerService } from './change-stream.service'

@Module({
  imports: [DatabaseModule]
	providers: [ChangeStreamBrokerService, EnrollmentsService,],
	exports: [ChangeStreamBrokerService, EnrollmentsService,],
})
export class MessagingModule {}
```

Importe o MessagingModule no módulo principal, ou no módulo cuja as classes dependem de ChangeStreamBrokerService.