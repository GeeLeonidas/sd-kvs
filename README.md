## Problemas não previstos pelo plano do projeto
- Não existe forma de verificar a resiliência do servidor e reverter uma alteração de valor/timestamp em caso de falha

## Ambiente utilizado
- JDK 1.8 (Eclipse Temurin)
- Linux (kernel 6.1.37-xanmod1)

## Instruções
- Executando o comando `gradlew compileJava`, os arquivos .class serão gerados na pasta `build/classes/java/main`
- Para iniciar o servidor, execute `CLASSPATH="build/classes/java/main" java br.dev.gee.Servidor`
  - Inicialize o servidor inserindo o endereço e a porta em que deseja disponibilizá-lo
  - Informe qual é o endereço e porta do servidor líder (insira os mesmos valores anteriores caso trate-se do líder)
- Para iniciar o cliente, execute `CLASSPATH="build/classes/java/main" java br.dev.gee.Cliente`
  - Insira o endereço e a porta em que o servidor está rodando
  - Escolha uma das quatro opções disponíveis (`INIT`, `PUT`, `GET`, `EXIT`)
    - `INIT` recebe como entrada os endereços e portas dos três servidores disponíveis
    - `PUT` recebe uma chave e um valor como entrada e realiza uma requisição `PUT` para um dos três servidores aleatoriamente
    - `GET` recebe uma chave como entrada e realiza uma requisição `GET` para um dos três servidores aleatoriamente
    - `EXIT` termina o programa com código 0
