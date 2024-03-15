## Introdução
Atualmente, com a proliferação de sistemas e a adoção crescente do conceito de microsserviços, as pilhas de tecnologia estão expandindo-se exponencialmente. Esse crescimento traz consigo uma complexidade cada vez maior na manutenção desses sistemas, garantindo sua consistência e funcionalidade. Após trabalhar em diversos projetos com essas características, desenvolvemos uma solução de [captura de alterações em banco de dados](https://en.wikipedia.org/wiki/Change_data_capture), uma estratégia de integração de sistemas que se baseia na leitura de um "livro de registro" do banco de dados e na transmissão das alterações para os sistemas que necessitam acompanhar tais mudanças.
Na nossa arquitetura, empregávamos o banco de dados Postgres, que possui um registro conhecido como WAL (Write Ahead Log). O componente de WAL do Postgres grava as operações de transação em um formato binário simples, o que permite ao banco realizar limpezas ou replicar dados de maneira consistente. Além do Postgres, também utilizamos sistemas como o Debezium, que permite a leitura do WAL do Postgres e o armazenamento dos dados em um Kafka, tornando-os disponíveis para outros sistemas.
Com o desenvolvimento dessa arquitetura, despertou em mim a curiosidade de compreender mais profundamente seu funcionamento interno. Queria entender o que acontecia em um nível mais baixo e como poderíamos implementar uma ferramenta simples que permitisse exportar dados para YAML. Foi assim que surgiu o [pgoutput2yml](https://github.com/bernardo-bruning/pgoutput2yml).
O pgoutput é uma ferramenta incrivelmente simples, com apenas dois comandos, ela nos possibilita capturar as alterações geradas no banco de dados e exportá-las para um arquivo YAML externo.
Para configura-lo habilite o wal_level logic no postgres :
```
  ALTER SYSTEM SET wal_level = logical;
```
Com o wal_level habilitado instale o pgoutput2yml.
```
  pgoutput2yml --host $HOST --user $USER --password $PASSWORD --install
```
Após a instalação do pgoutput2yml podemos iniciar sua execução com o comando:
```
  pgoutput2yml --host $HOST --user $USER --password $PASSWORD
```
Você verá suas alterações sendo exibidas no console da seguinte forma:
```
  ---
  relation_id: 16386:
  operation: relation  
  namespace: public  
  name: product  
  replica_identity_settings: 102  
  columns:  
    - id
    - name
  ---
  relation_id: 16386
  operation: insert  
  data:  
    - 1
    - EcoGlow Energy Saver
  ---
```
Para disponibilizar essa implementação, foram utilizadas as bibliotecas padrão libpq, aplicando os conceitos discutidos anteriormente, os quais serão abordados em mais detalhes nas seções abaixo. Embora os conceitos sejam apresentados usando a biblioteca `libpq` do C, eles podem ser reproduzidos em outras linguagens. No entanto, é importante observar que linguagens como Java podem já possuir todo o mecanismo implementado (consulte [a documentação do JDBC do PostgreSQL](https://jdbc.postgresql.org/documentation/server-prepare/#logical-replication)).
## Configurações inciais
Para iniciarmos o protocolo de replicação precisamos inicialmente configurar uma **publication** e um **slot de replicação**, possuindo estes dois componentes podemos então enviar um comando de `START_REPLICATION` ao postgres, permitindo abrir um streaming de copy entre o replicador e o banco de dados replicado.
Para iniciar o protocolo de replicação, é necessário configurar inicialmente uma **publication** e um **replication slot**. Com esses dois componentes configurados, podemos então enviar um comando `START_REPLICATION` ao PostgreSQL, o que permite abrir um fluxo de cópia entre o replicador e o banco de dados replicado.
A construção de uma publication envolve a definição das tabelas cujas alterações desejamos publicar, assim como os tipos de comandos a serem incluídos na replicação, como `INSERT`, `DELETE`, `UPDATE` e `TRUNCATE`, e como serão utilizadas as chaves primárias para o serviço de replicação. Para estabelecer uma publicação para todas as tabelas e operações, podemos executar o seguinte comando:
```
  CREATE PUBLICATION my_publication FOR ALL TABLES;
```
Com as configurações da publication feitas, agora precisamos configurar nosso slot lógico. Esse slot permite identificar em qual posição do WAL do PostgreSQL estamos lendo nossas informações, o que também possibilita retomar sua execução em caso de falha. Além disso, o slot de replicação determina qual protocolo será utilizado para a replicação de dados. Em nosso exemplo, estamos utilizando o `pgoutput`, mas o PostgreSQL também oferece nativamente o `test_decoding`, além da possibilidade de instalar decoders diferentes, como o wal2json.
Para criar nosso slot, executamos o seguinte comando:
```
  SELECT pg_create_logical_replication_slot('my_slot', 'pgoutput');
```
## Iniciando replicação
  Antes de acionar uma replicação nossa conexão com o banco de dados recebe a [configuração](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/main.c#L98) `replication=database` permitindo assim abrir streamings de replicação entre o nosso cliente e o banco de dados. Podemos abrir conexões similares utilizando `psql` como o exemplo abaixo.  
```
  psql -U $USER "replication=database" -h $HOST
```
Com nosso conexão estabelecida iniciamos o streaming de replicação simplesmente mandando o comando.
```
  START_REPLICATION SLOT my_slot LOGICAL 0/0 (proto_version '1', publication_names 'my_publication');
```
Esse comando permite abrir um streaming para transferência de dados, similar ao comando COPY, porém com a capacidade de enviar e receber dados por meio de um único canal. Isso possibilita ao replicador receber dados da WAL em formato binário estabelecido pelo pgoutput, bem como enviar ACKs (acknowledgments) e atualizações de status para o PostgreSQL que está sendo replicado. Essa abordagem de streaming bidirecional é fundamental para a sincronização contínua e eficiente entre os sistemas de origem e destino na replicação de dados.
![image.png](../assets/image_1710298637473_0.png){:height 364, :width 363}
Ao utilizar a biblioteca `libpq`, após iniciar a replicação, podemos ler e transmitir dados usando os comandos `PQputCopyData` e `PQgetCopyData`. Esses comandos de COPY operam em um nível baixo, permitindo uma capacidade de transmissão de informações da maneira mais otimizada possível. Eles enviam apenas dados binários, o que nos permite ler essas informações do canal de comunicação de forma eficiente e direta. Essa abordagem oferece um meio eficaz para trocar dados entre o replicador e o banco de dados replicado, minimizando o overhead e maximizando o desempenho da replicação.
## Lendo dados do streaming de comunicação
Um streaming, assim como qualquer outra comunicação em nível de máquina, opera essencialmente com dados binários. Portanto, para lidar com um streaming de cópia (COPY), é necessário ler os dados binários disponibilizados nos buffers de `PQgetCopyData` e `PQputCopyData`. Esses buffers facilitam a troca eficiente de dados binários entre o cliente e o servidor PostgreSQL durante a replicação. Essa abordagem é essencial para garantir uma comunicação robusta e de alto desempenho entre os sistemas envolvidos, exigindo compreensão e manipulação adequadas dos dados binários para uma replicação precisa e confiável.
Lidar com dados como textos ou caracteres em C é relativamente simples, pois o mesmo trabalha com o ASCII, onde cada caractere é representado por apenas 1 byte. No entanto, ao lidar com informações como números inteiros, é necessário realizar uma conversão de big-endian para o formato suportado pelo host. Isso é feito utilizando funções como `beXtoh`, que convertem os dados para o formato correto antes de serem manipulados.
Parte das conversões de arquivos binários são realizadas como um streaming de dados e são convertidas através do arquivo [stream.c](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/stream.c). Este arquivo contém as implementações necessárias para ler os dados do streaming, interpretá-los corretamente e convertê-los para o formato adequado para serem utilizados pela aplicação.
Com a capacidade de converter informações binárias em dados nativos em C, precisamos ler as principais mensagens do streaming de dados. Vamos abordar as duas mensagens principais: as mensagens contendo dados da WAL e as mensagens de keep-alive.
Essas mensagens começam com caracteres específicos que indicam o tipo de mensagem que estão representando, permitindo-nos identificar e interpretar corretamente o conteúdo. Esse processo é fundamental para a leitura eficiente e precisa das informações transmitidas pelo streaming de dados.
#### Mensagens com dados da WAL
A mensagens com dados da WAL é praticamente formado pelos seguintes dados descritos abaixo.
	1. Caracter 'w', identificador de que a mensagem é sobre dados da WAL.
	2. Int64, posição inicial na WAL dos dados que estarão sendo recebidos.
	3. Int64, posição final da WAL no servidor
	4. Int64, relogio do servidor, baseado em microsegundos, iniciando em 2000-01-01
	5. N bytes com informações da WAL, dados estes que serão tratados posteriormente.
Ao receber tais mensagens o sistema replicador não precisa reenviar qualquer mensagem do tipo, podendo assim requisitar novos dados.
#### Mensagens de keep-alive
As mensagens de keep-alive são formadas de maneira similar descrito abaixo.
	1. Caracter 'k', identificador de que a mensagem é sobre keep-alive
	2. Int64, posição final da WAL no servidor
	3. Int64, relogio do servidor, baseado em microsegundos, iniciando em 2000-01-01
	4. Booleano baseado em 1 byte, quando 1 o cliente deve responder a mensagem o mais rápido possível para evitar desconexão
#### Mensagens de atualização de status
Quando o cliente ao receber mensagens de keep-alive precisamos no primeiro instante garantir e enviar uma resposta ao servidor.  Para enviar esta resposta precisamos enviar uma resposta solicitando uma atualização de status, conforme descrito abaixo:
	1. Caracter 'r', identificador da mensagem para atualizar um status.
	2. Int64, A localição da ultima WAL +1 lida e escrita em disco.
	3. Int64, A localização da ultima WAL +1 descarregado para o disco
	4. Int64, A localização da ultima WAL +1 aplicada
	5. Int64, relogio do servidor, baseado em microsegundos.
	6. Booleano baseado em 1 byte, quando 1 o cliente solicita para o servidor responder a mensagem imediatamente, caso diferente o mesmo apenas da baixa deste valor.
Tais operações de leituras são realizadas [aqui](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/main.c#L188-L202) no pgoutput2yml.
## Formato de dados WAL
Os dados da WAL seguem o mesmo formato binário, sendo uma extensão das definições projetadas acima. Tais definições estão [documentadas pelo postgresql](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html) e todo e qualquer comando possui uma palavra que indica sobre qual mensagem estamos falando. Para fins de simplificar minha explicação trarei um exemplo simples. 
  
  Imagine que você execute a seguinte operação:  
```
  INSERT INTO table_test(id, value) VALUES(1, 'test');
```
Esta operação irá gerar duas 4 mensagens em nossa WAL, sendo elas de [Begin](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-BEGIN), [Relation](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-RELATION), [Insert](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-INSERT), [Commit](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-COMMIT). Tais mensagens refletem as operações que foram executadas internamente no banco de dados mesmo que o usuário não tenha definido em seu input, as mensagens de Begin e Commit representam a transação, a mensagem de Relation representa a relação de uma operação com uma tabela, e a de insert de fato representa a operação de inserir no banco de dados.
Considerando que essas operações vem na ordem descrita, nossa aplicação de replicação irá ver os seguintes dados, representado em caracteres, sendo cada espaço um dado recebido, omitindo seu tipo:
```
  B 1 1710542234 1
  R 1 1 public table_test 102 2 0 id 2 0 0 value 3 0
  I 1 1 N 2 t 1 t test
  C 0 1 2 1710542238
```
Ao olhar para essa representação fica evidente que conseguimos ler alguns textos contendo informações do nosso insert, mas podemos pegar alguns detalhes interessante. Informações de reletion acabam tando a seguinte representação abaixo
```
  R 1 1 public table_test 102 2 [0 id 2 0] [0 value 3 0]
  ^ ^ ^ ^      ^          ^   ^  ^ ^  ^ ^  ^ 
  | | | |      |          |   |  | |  | |  | Mesma estrutura por ser uma nova coluna
  | | | |      |          |   |  | |  | | Representa um modificador da coluna
  | | | |      |          |   |  | |  | Representa OID do tipo de dados
  | | | |      |          |   |  | | Representa o nome da coluna
  | | | |      |          |   |  | Flag identificando se é uma chave primaria 
  | | | |      |          |   | Número de colunas        
  | | | |      |          | Identidade de replicação
  | | | |      | Nome da tabela          
  | | | | Namespace                                  
  | | | OID da relaçao                                    
  | | ID da transação
  | Caracter de identificação da relação                                        
```
Parte desse processo de tradução acontece na aplicação através do seguinte [código](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/main.c#L51-L82) enviando suas traduções para o [decoder.c](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/decoder.c).
## Confirmando informações já replicadas
Após decodificar a mensagem precisamos confirma-las em disco, ou a um sistema terceira, e então atualizar nosso status no slot de replicação. Normalmente partindo de uma replica do postgres isso acontece em cada operação commit que o servidor de replica recebe, o banco irá aplicar a operação de [flush](https://pubs.opengroup.org/onlinepubs/000095399/functions/fflush.html) garantindo que os dados em memoria ou em cache no sistema operacional tenham sido gravados em disco.
Com tais confirmações em disco é seguro enviar uma mensagem de atualização de status antes mesmo de ler as próximas informações disponíveis na WAL, garantindo assim que em caso de uma queda de energia informações não sejam perdidas ou repetidas. Na implementação do pgoutput2yml não possuem garantias de não repetição de mensagens, mas as mesmas são reduzidas com as práticas comentadas, bancos de dados replicados muitas vezes fazem verificação se a posição da WAL já foi ou não confirmada em disco, evitando problemas de duplicidade.
Tais operações de garantias no pgoutput2yml é feito no momento de commit implementado [aqui](https://github.com/bernardo-bruning/pgoutput2yml/blob/main/src/main.c#L57-L58).
## Conclusão
Conforme discutido neste artigo, lidar com operações da WAL de forma nativa requer cuidados específicos e pode ser uma tarefa desafiadora. No entanto, existem boas abstrações disponíveis no mercado, como o Debezium, que não apenas facilita a integração direta com o PostgreSQL, mas também oferece suporte para outras bases de dados, como MySQL, MongoDB e outras.
Entender os processos internos de um banco de dados e como os componentes estabelecem conexões é uma parte essencial para o diagnóstico e resolução de problemas. Espero que este artigo tenha contribuído para melhorar sua compreensão sobre como funcionam os mecanismos de Change Data Capture (CDC) e como podemos aproveitá-los de maneira eficaz em nossos projetos.
