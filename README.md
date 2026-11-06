# KeyValueStore em Python

Armazenamento chave-valor persistente em Python puro, construído sobre um **log de escrita antecipada (write-ahead log) append-only**. Cada escrita é gravada no log com checksum CRC32 antes de alterar a memória; na abertura o log é reexecutado, descartando qualquer cauda truncada ou corrompida (recuperação de falhas), e uma compactação atômica reescreve o log só com as chaves vivas. Inclui CLI.

## Por que este projeto

Quase todo banco de dados e sistema de mensageria (PostgreSQL, SQLite em modo WAL, Kafka, LevelDB, Bitcask, etcd) é variação do mesmo padrão: gravar sequencialmente num log e reconstruir o estado a partir dele. Perguntas de entrevista sobre durabilidade ("o que acontece se a máquina cair no meio de uma escrita?", "quando chamar fsync?", "como evitar que o log cresça sem limite?") têm resposta direta neste código. É também um bom exemplo de testar falhas de verdade: a suíte corta o arquivo em todos os pontos possíveis e confirma que o resultado é sempre um prefixo válido das escritas.

## Recursos

- `put`, `get`, `delete` (com marcador de remoção, o tombstone), `keys`, `items`, `len`, `in` e uso como gerenciador de contexto.
- Formato binário de registro: CRC32, operação, tamanhos e dados; chaves em UTF-8 e valores binários arbitrários.
- Recuperação de falhas: cauda truncada (escrita interrompida) ou com checksum inválido é removida na abertura, e o log volta a terminar em um limite de registro. `store.recovery` informa registros reaplicados e bytes descartados.
- `sync=True` (padrão) executa `fsync` a cada escrita; `sync=False` troca durabilidade por velocidade.
- Compactação manual (`compact()`) e automática (`auto_compact_ratio`), com arquivo temporário, `fsync` e `os.replace` atômico.
- Arquivo temporário deixado por uma compactação interrompida é ignorado e removido.
- Seguro para uso com várias threads (um `RLock`).
- CLI: `put`, `get`, `delete`, `keys`, `compact`, `stats`.

## Como executar

Requer Python 3.8 ou superior. Dentro da pasta do projeto:

```bash
python3 -m kvstore --path kv.db put cidade "Sao Paulo"
python3 -m kvstore --path kv.db get cidade
python3 -m kvstore --path kv.db keys
python3 -m kvstore --path kv.db stats
python3 -m kvstore --path kv.db compact
python3 -m kvstore --path kv.db delete cidade
```

`--no-sync` desliga o `fsync` por escrita. Como biblioteca:

```python
from kvstore import KeyValueStore

with KeyValueStore("kv.db", auto_compact_ratio=8) as store:
    store.put("usuario:1", "ana")
    print(store.get("usuario:1"))      # b'ana'
    print(store.recovery)              # o que foi reaplicado/descartado na abertura
```

## Como testar

```bash
python3 -m unittest -v
```

A suíte inclui: ida e volta de dados e remoções após reabrir; processo "morto" sem `close()`; escrita parcial no fim do log; **truncamento em todos os offsets possíveis**, verificando que o estado recuperado é sempre um prefixo das escritas; byte corrompido no meio do log; arquivo com lixo; compactação (tamanho, dados, escrita posterior, arquivo temporário órfão, compactação automática); seis threads escrevendo ao mesmo tempo; e a CLI.

## Decisões de projeto

- **O log é a fonte da verdade.** A escrita vai primeiro ao arquivo (e ao disco, com `sync`) e só depois ao dicionário em memória. Se o processo cair entre os dois passos, o log já tem o dado; o contrário nunca acontece.
- **Parar no primeiro registro inválido.** Depois de um registro corrompido não se pode confiar em nada que o segue (os limites de registro se perderam), então o log é truncado ali. Isso pode perder escritas posteriores a uma corrupção no meio do arquivo, mas nunca devolve dados errados.
- **CRC32 sobre cabeçalho e dados.** Detecta escrita rasgada e corrupção de bits. Não protege contra adulteração intencional.
- **Limite de tamanho por campo.** Um comprimento corrompido não pode provocar uma leitura gigante.
- **Compactação por substituição atômica.** O arquivo novo é completo e gravado em disco antes do `os.replace`; uma queda durante a compactação deixa o log original intacto. O diretório também recebe `fsync` para tornar a troca durável.
- **Índice inteiro em memória (valores incluídos).** Leituras são O(1) e a recuperação é uma varredura linear O(tamanho do log). Bitcask guarda apenas offsets na memória; aqui os valores ficam na memória por simplicidade, o que limita o tamanho do conjunto de dados à RAM.
- **Custos:** `put`/`delete` O(1) amortizado mais o `fsync`; `compact` O(chaves vivas); abertura O(registros).
- **Trade-off do `fsync` por escrita:** é o que garante durabilidade e também o que limita a vazão; sistemas reais agrupam escritas (group commit) para amortizar esse custo.

## Estrutura de pastas

```
KeyValueStore/
├── kvstore/
│   ├── __init__.py
│   ├── __main__.py
│   ├── wal.py          # formato de registro, CRC, leitura tolerante a falhas
│   ├── store.py        # KeyValueStore, recuperação, compactação
│   └── cli.py
├── tests/
│   ├── test_store.py
│   └── test_cli.py
├── LICENSE
└── README.md
```

## Limitações conhecidas

- Todos os dados cabem na memória; não há índice em disco nem segmentos.
- Um único processo por arquivo: não há trava de arquivo, e dois processos abertos ao mesmo tempo corromperiam o log.
- Sem transações em várias chaves nem snapshots.
- Sem group commit: com `sync=True` cada escrita paga um `fsync`.
- Corrupção no meio do log faz descartar também os registros válidos que vêm depois.
- Sem compressão nem criptografia.

## Licença

MIT. Veja o arquivo [LICENSE](LICENSE).
