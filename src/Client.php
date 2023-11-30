<?php

namespace LinkORB\Component\Etcd;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\ClientInterface;
use GuzzleHttp\Exception\BadResponseException;
use LinkORB\Component\Etcd\Exception\EtcdException;
use LinkORB\Component\Etcd\Exception\KeyExistsException;
use LinkORB\Component\Etcd\Exception\KeyNotFoundException;
use RecursiveArrayIterator;
use RuntimeException;
use stdClass;

class Client
{
    private ClientInterface $guzzleClient;
    private string $apiVersion;
    private string $root = '';
    private array $dirs = [];
    private array $values = [];

    public function __construct(string $server = '', string $version = 'v2', ?ClientInterface $client = null)
    {
        $server = rtrim($server, '/');

        if (!$server) {
            $server = 'http://127.0.0.1:2379';
        }

        $this->apiVersion = $version;
        $this->guzzleClient = $client ?: new GuzzleClient(
            [
                'base_uri' => $server,
            ]
        );
    }


    /**
     * Set the default root directory. the default is `/`
     * If the root is others e.g. /linkorb when you set new key,
     * or set dir, all of the key is under the root
     * e.g.
     * <code>
     *    $client->setRoot('/linkorb');
     *    $client->set('key1, 'value1');
     *    // the new key is /linkorb/key1
     * </code>
     * @param string $root
     * @return Client
     */
    public function setRoot(string $root): Client
    {
        if (strpos($root, '/') !== 0) {
            $root = '/' . $root;
        }
        $this->root = rtrim($root, '/');

        return $this;
    }


    /**
     * get server version
     * @param string $uri
     * @return mixed
     */
    public function getVersion(string $uri)
    {
        $response = $this->guzzleClient->get($uri);

        try {
            $data = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        return $data;
    }

    /**
     * Set the value of a key
     * @param string   $key
     * @param string   $value
     * @param int|null $ttl
     * @param array    $condition
     * @return mixed
     */
    public function set(string $key, string $value, int $ttl = 0, array $condition = [])
    {
        $data = ['value' => $value];

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        try {
            $response = $this->guzzleClient->put($this->buildKeyUri($key), [
                'query' => $condition,
                'form_params' => $data,
            ]);
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        return $body;
    }

    /**
     * Retrieve the value of a key
     * @param string $key
     * @param array  $query the extra query params
     * @return array
     * @throws KeyNotFoundException
     */
    public function getNode(string $key, array $query = []): array
    {
        try {
            $response = $this->guzzleClient->get(
                $this->buildKeyUri($key),
                [
                    'query' => $query,
                ]
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }
        return $body['node'];
    }

    /**
     * Retrieve the value of a key
     * @param string $key
     * @param array  $flags the extra query params
     * @return string the value of the key.
     * @throws KeyNotFoundException
     */
    public function get(string $key, array $flags = []): string
    {
        $node = $this->getNode($key, $flags);

        return $node['value'];
    }

    /**
     * make a new key with a given value
     *
     * @param string $key
     * @param string $value
     * @param int    $ttl
     * @return array $body
     * @throws KeyExistsException
     */
    public function mk(string $key, string $value, int $ttl = 0): array
    {
        $body = $this->set(
            $key,
            $value,
            $ttl,
            ['prevExist' => 'false']
        );

        if (isset($body['errorCode'])) {
            throw new KeyExistsException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * make a new directory
     *
     * @param string $key
     * @param int    $ttl
     * @return array $body
     * @throws KeyExistsException
     */
    public function mkdir(string $key, int $ttl = 0): array
    {
        $data = ['dir' => 'true'];

        if ($ttl) {
            $data['ttl'] = $ttl;
        }
        try {
            $response = $this->guzzleClient->put(
                $this->buildKeyUri($key),
                [
                    'query' => ['prevExist' => 'false'],
                    'form_params' => $data,
                ]
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }


        if (isset($body['errorCode'])) {
            throw new KeyExistsException($body['message'], $body['errorCode']);
        }
        return $body;
    }


    /**
     * Update an existing key with a given value.
     * @param string $key
     * @param string $value
     * @param int    $ttl
     * @param array  $condition The extra condition for updating
     * @return array $body
     * @throws KeyNotFoundException
     */
    public function update(string $key, string $value, int $ttl = 0, array $condition = []): array
    {
        $extra = ['prevExist' => 'true'];

        if ($condition) {
            $extra = array_merge($extra, $condition);
        }
        $body = $this->set($key, $value, $ttl, $extra);
        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }
        return $body;
    }

    /**
     * Update directory
     * @param string $key
     * @param int    $ttl
     * @return array $body
     * @throws EtcdException
     */
    public function updateDir(string $key, int $ttl): array
    {
        if (!$ttl) {
            throw new EtcdException('TTL is required', 204);
        }

        $condition = [
            'dir' => 'true',
            'prevExist' => 'true',
        ];

        $response = $this->guzzleClient->put(
            $this->buildKeyUri($key),
            [
                'query' => $condition,
                'form_params' => [
                    'ttl' => $ttl,
                ],
            ]
        );

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        if (isset($body['errorCode'])) {
            throw new EtcdException($body['message'], $body['errorCode']);
        }
        return $body;
    }


    /**
     * remove a key
     * @param string $key
     * @return array|stdClass
     * @throws EtcdException
     */
    public function rm(string $key)
    {
        try {
            $response = $this->guzzleClient->delete($this->buildKeyUri($key));
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        if (isset($body['errorCode'])) {
            throw new EtcdException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Removes the key if it is directory
     * @param string  $key
     * @param boolean $recursive
     * @return mixed
     * @throws EtcdException
     */
    public function rmdir(string $key, bool $recursive = false)
    {
        $query = ['dir' => 'true'];

        if ($recursive === true) {
            $query['recursive'] = 'true';
        }

        try {
            $response = $this->guzzleClient->delete(
                $this->buildKeyUri($key),
                [
                    'query' => $query,
                ]
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        if (isset($body['errorCode'])) {
            throw new EtcdException($body['message'], $body['errorCode']);
        }
        return $body;
    }

    /**
     * Retrieve a directory
     * @param string  $key
     * @param boolean $recursive
     * @return mixed
     * @throws KeyNotFoundException
     */
    public function listDir(string $key = '/', bool $recursive = false)
    {
        $query = [];
        if ($recursive === true) {
            $query['recursive'] = 'true';
        }

        $response = $this->guzzleClient->get(
            $this->buildKeyUri($key),
            [
                'query' => $query,
            ]
        );

        try {
            $body = json_decode((string)$response->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Retrieve a directories key
     * @param string  $key
     * @param boolean $recursive
     * @return array
     * @throws EtcdException
     */
    public function ls(string $key = '/', bool $recursive = false): array
    {
        $this->values = [];
        $this->dirs = [];

        $data = $this->listDir($key, $recursive);

        $iterator = new RecursiveArrayIterator($data);
        return $this->traverseDir($iterator);
    }

    /**
     * Get all key-value pair that the key is not directory.
     * @param string      $root
     * @param boolean     $recursive
     * @param string|null $key
     * @return array
     * @throws EtcdException
     */
    public function getKeysValue(string $root = '/', bool $recursive = true, string $key = null): array
    {
        $this->ls($root, $recursive);

        return $this->values[$key] ?? $this->values;
    }

    /**
     * create a new directory with auto generated id
     *
     * @param string $dir
     * @param int    $ttl
     * @return array $body
     */
    public function mkdirWithInOrderKey(string $dir, int $ttl = 0): array
    {
        $data = [
            'dir' => 'true',
        ];

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        $request = $this->guzzleClient->post(
            $this->buildKeyUri($dir),
            ['body' => $data],
        );

        try {
            $json = json_decode((string)$request->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        return $json;
    }

    /**
     * create a new key in a directory with auto generated id
     *
     * @param string $dir
     * @param string $value
     * @param int    $ttl
     * @param array  $condition
     * @return array $body
     */
    public function setWithInOrderKey(string $dir, string $value, int $ttl = 0, array $condition = []): array
    {
        $data = ['value' => $value];

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        $request = $this->guzzleClient->post($this->buildKeyUri($dir), [
            'body' => $data,
            'query' => $condition,
        ]);

        try {
            $json = json_decode((string)$request->getBody(), true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . $e->getMessage());
        }

        return $json;
    }

    /**
     * Traversal the directory to get the keys.
     * @param RecursiveArrayIterator $iterator
     * @return array
     */
    private function traverseDir(RecursiveArrayIterator $iterator): array
    {
        $key = '';
        while ($iterator->valid()) {
            if ($iterator->hasChildren()) {
                $this->traverseDir($iterator->getChildren());
            } else {
                if ($iterator->key() === 'key' && ($iterator->current() !== '/')) {
                    $this->dirs[] = $key = $iterator->current();
                }

                if ($iterator->key() === 'value') {
                    $this->values[$key] = $iterator->current();
                }
            }
            $iterator->next();
        }
        return $this->dirs;
    }

    /**
     * Build key space operations
     * @param string $key
     * @return string
     */
    private function buildKeyUri(string $key): string
    {
        if (strpos($key, '/') !== 0) {
            $key = '/' . $key;
        }

        return '/' . $this->apiVersion . '/keys' . $this->root . $key;
    }
}
