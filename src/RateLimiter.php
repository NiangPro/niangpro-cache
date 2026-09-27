<?php

declare(strict_types=1);

namespace Niang\Core;

use Niang\Core\Database\DB;

/**
 * Compteur de tentatives, sans dépendance à Redis/Memcached. Suit CACHE_DRIVER : sur fichier
 * (défaut), ou en base (table rate_limits) pour que la limite soit commune à tous les serveurs
 * web — sinon « 10 tentatives de connexion par minute » devient 10 × le nombre de serveurs.
 */
class RateLimiter
{
    /**
     * Lecture-puis-écriture protégée par un verrou couvrant tout le cycle (flock() sur le
     * descripteur ouvert, pas seulement au moment d'écrire) : sans ça, deux requêtes concurrentes
     * lisent le même compteur avant qu'aucune n'ait écrit sa mise à jour, et la seconde écriture
     * écrase la première — un incrément silencieusement perdu. Exactement le scénario qu'une
     * attaque par force brute par connexions parallèles (plutôt que séquentielles) exploiterait
     * pour affaiblir cette protection, l'un des usages principaux de ce compteur (voir
     * ThrottleRequests sur /login, /register, /forgot-password).
     */
    public static function attempt(string $key, int $maxAttempts, int $decaySeconds): bool
    {
        if (Cache::driver() === 'redis') {
            // INCR et EXPIRE dans un même script : atomique, même entre plusieurs serveurs.
            $count = Redis::connection()->eval(
                "local c = redis.call('INCR', KEYS[1]) if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return c",
                [self::redisKey($key)],
                [$decaySeconds]
            );

            return (int) $count <= $maxAttempts;
        }

        if (Cache::driver() === 'database') {
            return self::attemptInDatabase(sha1($key), $maxAttempts, $decaySeconds);
        }

        $handle = @fopen(self::path($key), 'c+');

        if ($handle === false) {
            throw new \RuntimeException("RateLimiter : impossible d'ouvrir " . self::path($key) . ' (droits sur storage/framework/ratelimits ?).');
        }

        flock($handle, LOCK_EX);

        $content = stream_get_contents($handle);
        $data = $content !== '' ? json_decode($content, true) : null;
        $now = time();

        if (!is_array($data) || $data['resetAt'] <= $now) {
            $data = ['count' => 0, 'resetAt' => $now + $decaySeconds];
        }

        $allowed = $data['count'] < $maxAttempts;

        if ($allowed) {
            $data['count']++;
        }

        ftruncate($handle, 0);
        rewind($handle);
        fwrite($handle, (string) json_encode($data));
        fflush($handle);
        flock($handle, LOCK_UN);
        fclose($handle);

        return $allowed;
    }

    public static function availableIn(string $key): int
    {
        if (Cache::driver() === 'redis') {
            return max(0, (int) Redis::command('TTL', self::redisKey($key)));
        }

        if (Cache::driver() === 'database') {
            $row = DB::selectOne('SELECT reset_at FROM rate_limits WHERE limit_key = ?', [sha1($key)], 'write');
            return $row ? max(0, (int) $row['reset_at'] - time()) : 0;
        }

        $data = self::read($key);
        return $data ? max(0, (int) $data['resetAt'] - time()) : 0;
    }

    /**
     * Même garantie que le verrou fichier, par la base : l'incrément conditionnel est une seule
     * instruction UPDATE, atomique — deux requêtes parallèles (même sur deux serveurs) ne peuvent
     * pas lire le même compteur puis écrire chacune « compteur + 1 ».
     */
    private static function attemptInDatabase(string $hash, int $maxAttempts, int $decaySeconds): bool
    {
        $now = time();

        DB::statement('DELETE FROM rate_limits WHERE limit_key = ? AND reset_at <= ?', [$hash, $now]);

        try {
            DB::statement(
                'INSERT INTO rate_limits (limit_key, attempts, reset_at) '
                . 'SELECT ?, 0, ? FROM (SELECT 1 AS one) AS np_seed WHERE NOT EXISTS (SELECT 1 FROM rate_limits WHERE limit_key = ?)',
                [$hash, $now + $decaySeconds, $hash]
            );
        } catch (\PDOException $e) {
            // Deux premières tentatives simultanées : l'autre requête a créé la ligne entre-temps.
            if (!str_starts_with((string) $e->getCode(), '23')) {
                throw $e;
            }
        }

        return DB::affected(
            'UPDATE rate_limits SET attempts = attempts + 1 WHERE limit_key = ? AND attempts < ?',
            [$hash, $maxAttempts]
        ) === 1;
    }

    /** Remet le compteur à zéro, ex. après une connexion réussie : RateLimiter::clear($cle). */
    public static function clear(string $key): void
    {
        if (Cache::driver() === 'redis') {
            Redis::command('DEL', self::redisKey($key));
            return;
        }

        if (Cache::driver() === 'database') {
            DB::statement('DELETE FROM rate_limits WHERE limit_key = ?', [sha1($key)]);
            return;
        }

        $path = self::path($key);

        if (is_file($path)) {
            unlink($path);
        }
    }

    /**
     * @internal appelée par Niang\Core\Testing\TestCase::setUp() : les tentatives d'un test (10
     * connexions par minute sur /login) ne doivent pas faire échouer un test suivant en 429.
     */
    public static function reset(): void
    {
        foreach (glob(base_path('storage/framework/ratelimits/*.json')) ?: [] as $file) {
            unlink($file);
        }
    }

    private static function redisKey(string $key): string
    {
        return Redis::connection()->key('ratelimit:' . sha1($key));
    }

    private static function path(string $key): string
    {
        $safe = preg_replace('/[^a-zA-Z0-9_.-]/', '_', $key);
        $dir = base_path('storage/framework/ratelimits');

        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }

        return "$dir/$safe.json";
    }

    private static function read(string $key): ?array
    {
        $path = self::path($key);

        if (!file_exists($path)) {
            return null;
        }

        $content = file_get_contents($path);
        return $content ? json_decode($content, true) : null;
    }
}
