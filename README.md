# example-flink-operator-chaining

Flink 오퍼레이터 체이닝이 언제 묶이고 언제 끊기는지 직접 확인하는 예제입니다.

파이프라인은 `Numbers → Triple → Even → PlusOne → Discard` 5개 연산으로 이루어져 있고, `--variant` 옵션으로 `Triple`과 `Even` 사이의 체이닝 조건을 하나씩 깨뜨립니다.

## 요구 사항

- Docker (Compose 포함)
- JDK 17 이상, Maven

## 빌드와 클러스터 실행

```bash
# 예제 Job 빌드 (target/chaining-example.jar 생성)
mvn -B package

# Flink 2.3.0 클러스터 실행 (JobManager 1, TaskManager 1, 슬롯 2)
docker-compose up -d
```

`target/` 디렉토리는 JobManager 컨테이너의 `/opt/flink/jobs`에 마운트됩니다. Web UI는 http://localhost:8081 에서 확인할 수 있습니다.

## Job 실행

```bash
docker-compose exec jobmanager ./bin/flink run -d /opt/flink/jobs/chaining-example.jar --variant default
```

| `--variant` | 바꾸는 것 | task 수 |
|---|---|---|
| `default` | 없음 | 1 |
| `no-chaining` | `env.disableOperatorChaining()` | 5 |
| `rebalance` | `Triple` 뒤에 `rebalance()` | 2 |
| `key-by` | `Triple` 뒤에 `keyBy()` | 2 |
| `parallelism` | `Even`의 병렬도를 2로 | 3 |
| `start-new-chain` | `Even`에 `startNewChain()` | 2 |
| `disable-chaining` | `Even`에 `disableChaining()` | 3 |
| `slot-group` | `Even`에 `slotSharingGroup("isolated")` | 2 |

`--records <N>`을 지정하면 N건을 처리하고 끝납니다. 지정하지 않으면 사실상 끝나지 않으므로 확인 후 `flink cancel <JobID>`로 취소합니다.

```bash
# 2억 건을 처리하고 종료 (체이닝 켬/끔 처리 시간 비교용)
docker-compose exec jobmanager ./bin/flink run -d /opt/flink/jobs/chaining-example.jar --variant no-chaining --records 200000000
```

## 정리

```bash
docker-compose down
```
