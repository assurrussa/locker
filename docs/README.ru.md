# Locker

`locker` — небольшая Go-библиотека для распределённой lease-блокировки. Ядро
управляет жизненным циклом лока через узкий интерфейс `Store`, а пакет `/redis`
даёт token-safe адаптер для go-redis.

Поддерживаются только два import path:

```text
github.com/assurrussa/locker
github.com/assurrussa/locker/redis
```

Минимальная версия — Go 1.24.

## Быстрый старт с Redis

```go
client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
defer client.Close()

store, err := lockerredis.New(client)
if err != nil {
	return err
}
locks, err := locker.New(store)
if err != nil {
	return err
}

err = locks.WithLock(ctx, "jobs:reindex", func(lockCtx context.Context) error {
	// Работа обязана завершаться при отмене lockCtx.
	return reindex(lockCtx)
})
if errors.Is(err, locker.ErrLost) {
	// Эксклюзивность дальнейшей работы больше не гарантирована.
}
return err
```

Redis-адаптер принимает обычный `*redis.Client`, `*redis.ClusterClient` или
`*redis.Ring`. Клиент остаётся собственностью приложения: адаптер его не
закрывает.

```sh
go get github.com/assurrussa/locker@v0.1.0
```

## Публичный контракт

```go
type Store interface {
	Acquire(context.Context, string, string, time.Duration) (bool, error)
	Refresh(context.Context, string, string, time.Duration) (bool, error)
	Release(context.Context, string, string) (bool, error)
}
```

Реализация `Store` обязана быть concurrency-safe, соблюдать отмену context и
не создавать lease короче запрошенного TTL.

Публичный lifecycle API ориентирован на lease:

```go
func (*Locker) Acquire(context.Context, string) (*Lease, error)
func (*Locker) TryAcquire(context.Context, string) (*Lease, bool, error)
func (*Lease) Release(context.Context) error
```

- `Acquire` повторяет попытки с exponential backoff до захвата или завершения
  context. После успешного acquire этот context больше не управляет временем
  жизни lease.
- `TryAcquire` делает одну попытку; занятый ключ возвращает
  `(nil, false, nil)`.
- `Lease.Release` освобождает lease только при совпадении ownership token и
  соблюдает caller context.
- `WithLock` и `WithTryLock` отменяют callback context при потере lease и
  выполняют cleanup на detached context, ограниченном operation timeout, после
  чего объединяют ошибки callback/cleanup через `errors.Join`.
- При panic callback выполняется best-effort release, затем повторно
  выбрасывается исходное panic-value без подмены, даже если cleanup тоже
  паникует. Deferred cleanup выполняется и при `runtime.Goexit` из callback.

Для обычных critical sections рекомендуется использовать `WithLock` и
`WithTryLock`. Явные `Acquire`/`TryAcquire` нужны для интеграции с более длинным
lifecycle компонента или ожидания `Lease.Lost()`.

Публичные sentinel errors: `ErrNotAcquired`, `ErrLost`, `ErrInvalidLease` и
`ErrNotOwner`. Проверяйте их через `errors.Is`, потому что одна ошибка может
содержать несколько причин.

## Потеря Lease

`Lease.Key()` возвращает исходный ключ, `Lease.Lost()` закрывается ровно один
раз при необратимой потере lease, а `Lease.Err()` сохраняет первую причину и
удовлетворяет `ErrLost`.

`Lost()` и `Err()` сообщают именно о неожиданной потере ownership. Успешный
`Release()` не закрывает `Lost()`, а `Err()` после него остаётся `nil`.

`Lease` нельзя копировать. Любая неоднозначная ошибка refresh/release делает
его потерянным.
`Refresh=false` означает потерю ownership. `Release=false` возвращает сразу
`ErrLost` и `ErrNotOwner`.

После transport error разрешён повторный `lease.Release`, но только для
cleanup. Даже успешный cleanup продолжает возвращать `ErrLost`. Lease сам
инкапсулирует владельца и не передаётся обратно в `Locker`; nil, incomplete,
конкурентно освобождаемый или уже освобождённый lease возвращает
`ErrInvalidLease`.

## Опции и дефолты

- TTL lease — 1 минута (`WithLeaseTTL`);
- refresh — 10 секунд (`WithRefreshInterval`);
- timeout операции Store — 10 секунд (`WithOperationTimeout`);
- retry backoff — номинально от 10 мс с жёстким максимумом 1 секунда
  (`WithRetryBackoff`);
- jitter — 20%, при этом итоговая задержка остаётся положительной и не
  превышает настроенный жёсткий максимум (`WithJitterRatio`);
- token — 20 криптографически случайных байт.

Сумма refresh interval и удвоенного operation timeout должна быть меньше TTL.
Дополнительный operation budget учитывает часть TTL, прошедшую до получения
ответа acquire/refresh. Общее время блокирующего acquire ограничивает context
вызывающего кода.

## Observer

`WithObserver` принимает `Observer` или `ObserverFunc`. События покрывают
attempt, contention, acquire, refresh, release, failure и loss.

Observer вызывается асинхронно и не блокирует acquire, refresh или release.
Все вызовы одного `Locker` сериализуются через одну bounded best-effort
очередь; один Observer, переданный разным экземплярам `Locker`, всё ещё может
вызываться конкурентно. Доставленные события одного lock flow сохраняют
порядок. При длительном backpressure могут отбрасываться routine attempt,
contention и успешные refresh events. Observer всё равно обязан быстро
возвращаться; его panic изолируется от lifecycle лока. `Event.Key` содержит raw
application key; он может быть чувствительным и high-cardinality, поэтому его
нельзя без контроля использовать как metric label.
Wrapper-ошибки самой библиотеки не включают raw key или Redis storage key.
Пользовательские реализации `Store` и Redis `Resolver` самостоятельно отвечают
за очистку возвращаемых ими ошибок.

## Шардированный Redis

`redis.NewSharded` принимает resolver вида:

```go
func(context.Context, storageKey string) (redis.Client, error)
```

Resolver получает уже вычисленный storage key. Формат совместим с прежней
реализацией:

```text
locker:<первые-шесть-rune-или-none>:<16-символов-xxhash64>
```

Resolver обязан детерминированно и одинаково во всех процессах направлять
каждый storage key на один shard в течение всего времени жизни lease. Acquire,
refresh и release маршрутизируются независимо, поэтому изменение mapping при
живых lease может создать две независимые блокировки для одного логического
ресурса. Изменение topology shard'ов требует явной стратегии миграции,
сохраняющей routing живых ключей.

Acquire использует `SET NX` с TTL, а refresh/release — атомарные Lua-скрипты с
проверкой token. Дробная миллисекундная часть TTL округляется вверх, поэтому
Redis lease не становится короче запрошенного.

## Границы безопасности

Это lease mutex поверх одного Store. Блокировка не reentrant и не fair. В
`v0.1` нет fencing tokens, quorum/Redlock и защиты от split-brain при Redis
failover. Если stale writer недопустим, fencing или другой monotonic guard
должен проверяться самим защищаемым ресурсом.

Callback обязан реагировать на context: библиотека не может остановить код,
который продолжает работу после потери lease.

## Разработка

```sh
make verify
LOCKER_REDIS_ADDRS=127.0.0.1:6379,127.0.0.1:6380 make test-integration-race
```

Полные release-gates описаны в [CONTRIBUTING.md](../CONTRIBUTING.md).

## Лицензия

Locker распространяется по [лицензии MIT](../LICENSE).
