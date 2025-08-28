# Change Stream Broker

## 🌐 Português

### O que é o Projeto

**Change Stream Broker** é um pacote Node.js que transforma MongoDB Change Streams em um sistema completo de message broker para arquiteturas de microsserviços. Ele fornece uma API similar ao Kafka/RabbitMQ, mas utilizando apenas MongoDB como backbone.

#### Principais Características

- 🎯 **API Familiar**: Interface similar a brokers populares (Kafka-like).
- 📦 **Zero Dependências Extras**: Usa apenas o driver oficial do MongoDB.
- 🔄 **Resume Tokens**: Garante delivery exactly-once com mecanismo de offsets.
- 👥 **Consumer Groups**: Suporte a grupos de consumidores com balanceamento automático.
- 🛡️ **Type Safety**: Completo suporte a TypeScript com generics.
- ⚡ **Alta Disponibilidade**: Reconexão automática e lógica de retries integrada.
- 🔧 **Extensível**: Arquitetura modular para customizações.

#### Casos de Uso Ideais

- Microsserviços que já utilizam MongoDB.
- Sistemas que precisam de comunicação assíncrona entre serviços.
- Migração de sistemas legados para arquitetura orientada a eventos.
- Ambientes onde Kafka ou RabbitMQ seriam excessivos (overkill).

### Como Instalar

#### Pré-requisitos

- Node.js 16+  
- MongoDB 5.0+ com replica set habilitado  
- TypeScript (opcional, mas recomendado)  

#### Instalação

```bash
npm install @seu-usuario/change-stream-broker
```

#### Configuração do MongoDB

Para usar **Change Streams**, é necessário configurar um replica set:

- Utilize uma instância do **MongoDB Atlas Online**, que já vem com replica set habilitado.
- Ou configure um replica set localmente utilizando um container Docker. Consulte a documentação oficial do MongoDB para mais detalhes.

### Estrutura de Arquivos do Projeto

#### Principais Classes e Interfaces

```text
src/
├── index.ts                          # Ponto de entrada principal
├── broker/
│   ├── change-stream-broker.ts       # Classe principal do broker
│   ├── topic-manager.ts              # Gerenciador de tópicos
│   └── types.ts                      # Interfaces principais
├── consumer/
│   ├── consumer.ts                   # Implementação do consumer
│   ├── consumer.interface.ts         # Interface do consumer
│   ├── consumer-group.ts             # Gerenciador de consumer groups
│   └── message-handler.interface.ts  # Interface de handlers
├── producer/
│   └── producer.ts                   # Implementação do producer
└── storage/
    ├── offset-storage.ts             # Interface de storage
    ├── mongo-offset-storage.ts       # Storage de offsets no MongoDB
    └── file-offset-storage.ts        # Storage de offsets em arquivo
```

### Principais Interfaces

#### IChangeStreamConsumer

- **Definição**:  
```typescript
  interface IChangeStreamConsumer {
  getConsumerId(): string;
  getGroupId(): string;
  getTopic(): string;
  connect(): Promise<void>;
  subscribe<T = any>(config: MessageHandlerConfig<T>): Promise<void>;
  disconnect(): Promise<void>;
  commitOffsets(): Promise<void>;
}
```
A interface `IChangeStreamConsumer` define o contrato para a implementação de um consumidor de mensagens no **Change Stream Broker**. Ela garante que qualquer classe que implemente essa interface possua os métodos necessários para gerenciar a conexão, assinatura e processamento de mensagens. Abaixo estão os métodos definidos:

- **`getConsumerId(): string`**  
  Retorna o identificador único do consumidor. Esse ID é usado para distinguir consumidores individuais dentro de um grupo.

- **`getGroupId(): string`**  
  Retorna o identificador do grupo de consumidores ao qual este consumidor pertence. Consumidores no mesmo grupo compartilham a carga de processamento de mensagens.

- **`getTopic(): string`**  
  Retorna o nome do tópico ao qual o consumidor está associado. O tópico é a fonte das mensagens consumidas.

- **`connect(): Promise<void>`**  
  Estabelece a conexão do consumidor com o broker. Este método deve ser chamado antes de qualquer operação de assinatura ou consumo.

- **`subscribe<T = any>(config: MessageHandlerConfig<T>): Promise<void>`**  
  Permite que o consumidor se inscreva em um tópico com uma configuração específica de manipulador de mensagens (`MessageHandlerConfig`). O tipo genérico `<T>` define o formato esperado das mensagens.

- **`disconnect(): Promise<void>`**  
  Desconecta o consumidor do broker, encerrando a assinatura e liberando os recursos associados.

- **`commitOffsets(): Promise<void>`**  
  Confirma os offsets das mensagens processadas, garantindo que elas não sejam reprocessadas em caso de falhas ou reinicializações.

Essa interface é essencial para garantir que os consumidores sigam um padrão consistente, facilitando a implementação e manutenção do sistema.


#### MessageHandler

A interface `MessageHandler` define o contrato para a função responsável por processar mensagens consumidas de um tópico. Essa função é utilizada pelos consumidores para lidar com cada mensagem recebida. Abaixo está a explicação detalhada:

- **Definição**:  
  ```typescript
  interface MessageHandler<T = unknown> {
    (record: ConsumerRecord & { message: { value: T } }): Promise<void>;
  }
  ```
Essa interface é essencial para definir como as mensagens devem ser processadas pelos consumidores, permitindo que cada mensagem seja tratada de forma personalizada e assíncrona.

Tipo Genérico:
- **`<T = unknown>`**
Permite que o manipulador seja configurado para processar mensagens de diferentes formatos, garantindo flexibilidade e segurança de tipo.

Parâmetros:
- **`record`**  
  Um objeto que combina as propriedades de ConsumerRecord com uma mensagem (message) contendo um valor (value) do tipo genérico T.

- **`ConsumerRecord`**  
  Representa os metadados da mensagem consumida, como o tópico, partição e offset.

 - **`message.value`**  
  Contém o valor da mensagem, que é do tipo genérico T.


#### ConsumerRecord

A interface `ConsumerRecord` representa os metadados de uma mensagem consumida de um tópico. Esses metadados fornecem informações importantes sobre a origem e o contexto da mensagem, além de dados necessários para gerenciar o consumo. Abaixo está a explicação detalhada:

- **Definição**:  
  ```typescript
  interface ConsumerRecord {
    topic: string;
    partition: number;
    message: Message;
    offset: ResumeToken;
    timestamp: Date;
  }
  ```
Essa interface é fundamental para fornecer o contexto completo de uma mensagem consumida, permitindo que os consumidores processem as mensagens de forma eficiente e segura.

Propriedades:
- **`topic: string`**
O nome do tópico de onde a mensagem foi consumida.

- **`partition: number`**
O número da partição do tópico de onde a mensagem foi lida. Partições são usadas para distribuir mensagens e balancear a carga entre consumidores.

- **`message: Message`**
O conteúdo da mensagem consumida. A interface Message contém os dados principais que serão processados.

- **`offset: ResumeToken`**
O token de resume do MongoDB, usado para rastrear a posição da mensagem no stream. Esse token é essencial para garantir o processamento exatamente uma vez (exactly-once) e para retomar o consumo em caso de falhas.

- **`timestamp: Date`**
A data e hora em que a mensagem foi produzida ou consumida. Esse valor pode ser usado para fins de auditoria ou ordenação.


#### OffsetStorage

A interface `OffsetStorage` define o contrato para o gerenciamento de offsets no **Change Stream Broker**. Ela é responsável por armazenar e recuperar os offsets das mensagens consumidas, garantindo que o sistema possa retomar o consumo de onde parou em caso de falhas ou reinicializações. Abaixo está a explicação detalhada:

- **Definição**:  
  ```typescript
  interface OffsetStorage {
    commitOffset(commit: OffsetCommit): Promise<void>;
    getOffset(groupId: string, topic: string, partition: number): Promise<ResumeToken | null>;
  }
  ```
Essa interface é essencial para implementar diferentes estratégias de armazenamento de offsets, como armazenamento em MongoDB, arquivos ou outros sistemas, garantindo flexibilidade e confiabilidade no gerenciamento do consumo de mensagens.

Método:
- **`commitOffset(commit: OffsetCommit): Promise<void>`**
Armazena o offset mais recente para um grupo de consumidores, tópico e partição específicos.

Parâmetro:
- **`commit: OffsetCommit`**
Um objeto contendo as informações necessárias para registrar o offset, como o grupo de consumidores, tópico, partição e o token de resume.

Método:
- **`getOffset(groupId: string, topic: string, partition: number): Promise<ResumeToken | null>`**
Recupera o offset armazenado para um grupo de consumidores, tópico e partição específicos.

Parâmetros:
- **`groupId: string`** 
O identificador do grupo de consumidores.

- **`topic: string`**
O nome do tópico.

- **`partition: number`**
O número da partição.

Retorno:
- **`Promise<ResumeToken | null>`**
Retorno assíncrono do token de resume armazenado ou null se nenhum offset estiver disponível.


### Fluxo de Dados

```text
[Producer Service]          [MongoDB]               [Consumer Service]
     |                         |                         |
     | 1. Insere documento     |                         |
     |-----------------------> |                         |
     |                         |                         |
     |                         | 2. Change Stream detecta|
     |                         |    mudança              |
     |                         |-----------------------> |
     |                         |                         |
     |                         | 3. Processa mensagem    |
     |                         |    e commit offset      |
     |                         |<----------------------- |
```

### Configurações de Performance

O exemplo abaixo demonstra como configurar o **Change Stream Broker** com parâmetros otimizados para garantir alta performance e resiliência:

```typescript
// Exemplo de configuração otimizada
const broker = new ChangeStreamBroker({
  mongoUri: 'mongodb://localhost:27017', // URI de conexão com o MongoDB
  database: 'my-events',                // Nome do banco de dados utilizado
  maxRetries: 10,                       // Número máximo de tentativas de reconexão em caso de falhas
  retryDelayMs: 1000,                   // Intervalo (em milissegundos) entre as tentativas de reconexão
  heartbeatIntervalMs: 30000,           // Intervalo (em milissegundos) para envio de heartbeats
  autoCreateTopics: false               // Criação automática de tópicos desabilitada para evitar erro
});
```
Essa configuração é ideal para cenários onde a resiliência e a estabilidade são cruciais, garantindo que o sistema continue funcionando mesmo em situações de falhas temporárias.

Parâmetros:
- **`mongoUri`** 
Define a URI de conexão com o MongoDB. No exemplo, está configurado para um MongoDB local na porta padrão (27017).

- **`database`**
Especifica o banco de dados onde os eventos serão armazenados e gerenciados.

- **`maxRetries`**
Configura o número máximo de tentativas de reconexão em caso de falhas na comunicação com o MongoDB.

- **`retryDelayMs`**
Define o tempo de espera (em milissegundos) entre cada tentativa de reconexão.

- **`heartbeatIntervalMs`**
Determina o intervalo de envio de heartbeats para monitorar a conexão com o MongoDB.

- **`autoCreateTopics`**
Quando habilitado (true), permite que o broker crie tópicos automaticamente, caso eles não existam.


### Exemplo de Microsserviços com NestJS e Change Stream Broker

#### Estrutura do Projeto

```text
microservices-example/
├── purchase-service/
│   ├── src/
│   │   ├── purchases/
│   │   │   ├── purchases.module.ts
│   │   │   ├── purchases.service.ts
│   │   │   ├── purchases.controller.ts
│   │   │   └── schemas/
│   │   │       └── purchase.schema.ts
│   │   ├── change-stream/
│   │   │   └── purchase.publisher.ts
│   │   ├── app.module.ts
│   │   └── main.ts
│   ├── package.json
│   └── Dockerfile
├── classroom-service/
│   ├── src/
│   │   ├── enrollments/
│   │   │   ├── enrollments.module.ts
│   │   │   ├── enrollments.service.ts
│   │   │   ├── enrollments.controller.ts
│   │   │   └── schemas/
│   │   │       └── enrollment.schema.ts
│   │   ├── change-stream/
│   │   │   └── enrollment.consumer.ts
│   │   ├── app.module.ts
│   │   └── main.ts
│   ├── package.json
│   └── Dockerfile
├── docker-compose.yml
└── package.json
```

#### Serviço de Purchase (Producer)

purchase-service/ └── package.json

```json
{
  "name": "purchase-service",
  "version": "1.0.0",
  "dependencies": {
    "@nestjs/common": "^9.0.0",
    "@nestjs/core": "^9.0.0",
    "@nestjs/mongoose": "^9.0.0",
    "@nestjs/platform-express": "^9.0.0",
    "mongoose": "^6.0.0",
    "@dafaz/change-stream-broker": "^1.0.0"
  }
}
```

purchase-service/ └── src/ └── purchases/ └── schemas/ └── purchase.schema.ts


```typescript
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';

export type PurchaseDocument = Purchase & Document;

@Schema({ timestamps: true })
export class Purchase {
  @Prop({ required: true, type: MongooseSchema.Types.ObjectId, ref: 'Customer' })
  customerId: string;

  @Prop({ required: true, type: MongooseSchema.Types.ObjectId, ref: 'Product' })
  productId: string;

  @Prop({ required: true })
  productType: string;

  @Prop({ required: true })
  amount: number;

  @Prop({ default: 'pending' })
  status: string;

  @Prop()
  transactionId: string;
}

export const PurchaseSchema = SchemaFactory.createForClass(Purchase);
```

purchase-service/ └── src/ └── change-stream/ └── purchase.publisher.ts

```typescript
import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ChangeStreamBroker, ProducerConfig } from '@seu-usuario/change-stream-broker';

export interface PurchaseMessage {
  purchaseId: string;
  customerId: string;
  productId: string;
  productType: string;
  amount: number;
  status: string;
  createdAt: Date;
}

@Injectable()
export class PurchasePublisher implements OnModuleInit, OnModuleDestroy {
  private broker: ChangeStreamBroker;
  private producer: any;

  constructor() {
    this.broker = new ChangeStreamBroker({
      mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017',
      database: 'purchase-events',
      autoCreateTopics: true
    });
  }

  async onModuleInit() {
    await this.broker.connect();
    
    const producerConfig: ProducerConfig = {
      topic: 'purchases'
    };
    
    this.producer = await this.broker.createProducer(producerConfig);
    console.log('Purchase publisher initialized');
  }

  async publishPurchaseCreated(purchase: PurchaseMessage) {
    await this.producer.send({
      key: purchase.purchaseId,
      value: purchase,
      headers: {
        'event-type': 'purchase.created',
        'source': 'purchase-service'
      }
    });
    
    console.log(`Purchase event published: ${purchase.purchaseId}`);
  }

  async publishPurchaseStatusChanged(purchaseId: string, status: string) {
    await this.producer.send({
      key: purchaseId,
      value: { purchaseId, status, updatedAt: new Date() },
      headers: {
        'event-type': 'purchase.status.changed',
        'source': 'purchase-service'
      }
    });
  }

  async onModuleDestroy() {
    await this.broker.disconnect();
  }
}
```

purchase-service/ └── src/ └── purchases/ └── purchases.service.ts

```typescript
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Purchase, PurchaseDocument } from './schemas/purchase.schema';
import { PurchasePublisher } from '../change-stream/purchase.publisher';

@Injectable()
export class PurchasesService {
  constructor(
    @InjectModel(Purchase.name) private purchaseModel: Model<PurchaseDocument>,
    private purchasePublisher: PurchasePublisher
  ) {}

  async createPurchase(createPurchaseDto: any) {
    const purchase = new this.purchaseModel(createPurchaseDto);
    const savedPurchase = await purchase.save();

    // Publicar evento de purchase created
    await this.purchasePublisher.publishPurchaseCreated({
      purchaseId: savedPurchase._id.toString(),
      customerId: savedPurchase.customerId,
      productId: savedPurchase.productId,
      productType: savedPurchase.productType,
      amount: savedPurchase.amount,
      status: savedPurchase.status,
      createdAt: savedPurchase.createdAt
    });

    return savedPurchase;
  }

  async updatePurchaseStatus(purchaseId: string, status: string) {
    const updatedPurchase = await this.purchaseModel.findByIdAndUpdate(
      purchaseId,
      { status },
      { new: true }
    );

    if (updatedPurchase) {
      await this.purchasePublisher.publishPurchaseStatusChanged(
        purchaseId,
        status
      );
    }

    return updatedPurchase;
  }

  async findByCustomerId(customerId: string) {
    return this.purchaseModel.find({ customerId });
  }
}
```

purchase-service/ └── src/ └── purchases/ └── purchases.controller.ts

```typescript
import { Controller, Post, Body, Param, Patch, Get } from '@nestjs/common';
import { PurchasesService } from './purchases.service';

@Controller('purchases')
export class PurchasesController {
  constructor(private readonly purchasesService: PurchasesService) {}

  @Post()
  async create(@Body() createPurchaseDto: any) {
    return this.purchasesService.createPurchase(createPurchaseDto);
  }

  @Patch(':id/status')
  async updateStatus(@Param('id') id: string, @Body('status') status: string) {
    return this.purchasesService.updatePurchaseStatus(id, status);
  }

  @Get('customer/:customerId')
  async findByCustomer(@Param('customerId') customerId: string) {
    return this.purchasesService.findByCustomerId(customerId);
  }
}
```

purchase-service/ └── src/ └── app.module.ts

```typescript
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { PurchasesModule } from './purchases/purchases.module';
import { PurchasePublisher } from './change-stream/purchase.publisher';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/purchase-service'),
    PurchasesModule,
  ],
  providers: [PurchasePublisher],
  exports: [PurchasePublisher],
})
export class AppModule {}
```

#### Serviço de Classroom (Consumer)

classroom-service/ └── package.json

```json
{
  "name": "classroom-service",
  "version": "1.0.0",
  "dependencies": {
    "@nestjs/common": "^9.0.0",
    "@nestjs/core": "^9.0.0",
    "@nestjs/mongoose": "^9.0.0",
    "@nestjs/platform-express": "^9.0.0",
    "mongoose": "^6.0.0",
    "@seu-usuario/change-stream-broker": "^1.0.0"
  }
}
```

classroom-service/ └── src/ └── enrollments/ └── schemas/ └── enrollment.schema.ts

```typescript
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Schema as MongooseSchema } from 'mongoose';

export type EnrollmentDocument = Enrollment & Document;

@Schema({ timestamps: true })
export class Enrollment {
  @Prop({ required: true, type: MongooseSchema.Types.ObjectId, ref: 'Student' })
  studentId: string;

  @Prop({ required: true, type: MongooseSchema.Types.ObjectId, ref: 'Course' })
  courseId: string;

  @Prop({ required: true, unique: true })
  purchaseId: string;

  @Prop({ default: 'active' })
  status: string;

  @Prop()
  enrolledAt: Date;

  @Prop()
  completedAt: Date;
}

export const EnrollmentSchema = SchemaFactory.createForClass(Enrollment);
```

classroom-service/ └── src/ └── change-stream/ └── enrollment.consumer.ts

```typescript
import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { 
  ChangeStreamBroker, 
  ConsumerConfig, 
  MessageHandlerConfig 
} from '@seu-usuario/change-stream-broker';
import { EnrollmentsService } from '../enrollments/enrollments.service';

interface PurchaseMessage {
  purchaseId: string;
  customerId: string;
  productId: string;
  productType: string;
  amount: number;
  status: string;
  createdAt: Date;
}

@Injectable()
export class EnrollmentConsumer implements OnModuleInit, OnModuleDestroy {
  private broker: ChangeStreamBroker;

  constructor(private enrollmentsService: EnrollmentsService) {
    this.broker = new ChangeStreamBroker({
      mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017',
      database: 'purchase-events'
    });
  }

  async onModuleInit() {
    await this.broker.connect();

    const consumerConfig: ConsumerConfig = {
      groupId: 'classroom-service',
      topic: 'purchases',
      autoCommit: true,
      autoCommitIntervalMs: 5000,
      fromBeginning: false
    };

    const handlerConfig: MessageHandlerConfig<PurchaseMessage> = {
      handler: this.handlePurchaseEvent.bind(this),
      errorHandler: this.handleError.bind(this),
      maxRetries: 3,
      retryDelay: 1000,
      autoCommit: true
    };

    const consumer = await this.broker.createConsumer(consumerConfig);
    await consumer.subscribe(handlerConfig);

    console.log('Enrollment consumer started listening for purchase events...');
  }

  private async handlePurchaseEvent(record: any) {
    const purchase = record.message.value;
    
    // Apenas processar compras de cursos completadas
    if (purchase.productType === 'course' && purchase.status === 'completed') {
      console.log(`Processing course purchase: ${purchase.purchaseId}`);
      
      try {
        // Verificar se já existe matrícula para esta compra
        const existingEnrollment = await this.enrollmentsService.findByPurchaseId(purchase.purchaseId);
        
        if (!existingEnrollment) {
          // Criar matrícula automaticamente
          await this.enrollmentsService.create({
            studentId: purchase.customerId, // customerId é o studentId
            courseId: purchase.productId,   // productId é o courseId
            purchaseId: purchase.purchaseId,
            enrolledAt: new Date()
          });
          
          console.log(`Enrollment created for purchase: ${purchase.purchaseId}`);
        }
      } catch (error) {
        console.error(`Error creating enrollment for purchase ${purchase.purchaseId}:`, error);
        throw error; // Não commitar offset para reprocessar
      }
    }
  }

  private async handleError(error: Error, record?: any) {
    console.error('Error processing purchase event:', error);
    
    if (record) {
      console.error('Failed purchase record:', record.message.value);
      // Poderia enviar para uma DLQ aqui
    }
  }

  async onModuleDestroy() {
    await this.broker.disconnect();
  }
}
```

classroom-service/ └── src/ └── enrollments/ └── enrollments.service.ts

```typescript
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Enrollment, EnrollmentDocument } from './schemas/enrollment.schema';

@Injectable()
export class EnrollmentsService {
  constructor(
    @InjectModel(Enrollment.name) private enrollmentModel: Model<EnrollmentDocument>
  ) {}

  async create(createEnrollmentDto: any) {
    const enrollment = new this.enrollmentModel(createEnrollmentDto);
    return enrollment.save();
  }

  async findByPurchaseId(purchaseId: string) {
    return this.enrollmentModel.findOne({ purchaseId });
  }

  async findByStudentId(studentId: string) {
    return this.enrollmentModel.find({ studentId }).populate('courseId');
  }

  async updateStatus(enrollmentId: string, status: string) {
    return this.enrollmentModel.findByIdAndUpdate(
      enrollmentId,
      { status },
      { new: true }
    );
  }

  async completeEnrollment(enrollmentId: string) {
    return this.enrollmentModel.findByIdAndUpdate(
      enrollmentId,
      { 
        status: 'completed',
        completedAt: new Date()
      },
      { new: true }
    );
  }
}
```

classroom-service/ └── src/ └── enrollments/ └── enrollments.controller.ts

```typescript
import { Controller, Get, Post, Body, Param, Patch } from '@nestjs/common';
import { EnrollmentsService } from './enrollments.service';

@Controller('enrollments')
export class EnrollmentsController {
  constructor(private readonly enrollmentsService: EnrollmentsService) {}

  @Post()
  async create(@Body() createEnrollmentDto: any) {
    return this.enrollmentsService.create(createEnrollmentDto);
  }

  @Get('student/:studentId')
  async findByStudent(@Param('studentId') studentId: string) {
    return this.enrollmentsService.findByStudentId(studentId);
  }

  @Patch(':id/status')
  async updateStatus(@Param('id') id: string, @Body('status') status: string) {
    return this.enrollmentsService.updateStatus(id, status);
  }

  @Patch(':id/complete')
  async completeEnrollment(@Param('id') id: string) {
    return this.enrollmentsService.completeEnrollment(id);
  }
}
```

classroom-service/ └── src/ └── app.module.ts

```typescript
import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { EnrollmentsModule } from './enrollments/enrollments.module';
import { EnrollmentConsumer } from './change-stream/enrollment.consumer';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://localhost:27017/classroom-service'),
    EnrollmentsModule,
  ],
  providers: [EnrollmentConsumer],
})
export class AppModule {}
```

#### Exemplo Configuração Docker replSet (local - para desenvolvimento)

O Change Stream exige que a sua instância do MongoDB esteja em um cluster.
Para fins de desenvolvimento, você pode simular esse cluster. Vamos criar dois
arquivos para subir uma instância de dados no docker com replSet configurado.
**Observação: isso só deve ser feito para fins de desenvolvimento.**

[root] / └── docker-compose.yml

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

[root-path] / └── Dockerfile

```text
FROM mongo
RUN openssl rand -base64 756 > /etc/mongo-keyfile 
RUN chmod 400 /etc/mongo-keyfile 
RUN chown mongodb:mongodb /etc/mongo-keyfile 
```