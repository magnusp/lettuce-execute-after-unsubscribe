package lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

public class LettuceTest {
	private static       Logger log                     = LoggerFactory.getLogger(LettuceTest.class);
	// An alias that can be used to resolve the Toxiproxy container by name in the network it is connected to.
	// It can be used as a hostname of the Toxiproxy container by other containers in the same network.
	private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

	// Create a common docker network so that containers can communicate
	@Rule
	public Network network = Network.newNetwork();

	// The target container - this could be anything
	@Rule
	public GenericContainer redis = new GenericContainer("redis:latest")
		.withExposedPorts(6379)
		.withNetwork(network);

	// Toxiproxy container, which will be used as a TCP proxy
	@Rule
	public               ToxiproxyContainer                    toxiproxy        = new ToxiproxyContainer()
		.withNetwork(network)
		.withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
	private              ToxiproxyContainer.ContainerProxy     proxy;
	private static final String                                KEY              = "stub-key";
	private static final String                                VALUE_REDIS_UP   = "value-up";
	private static final String                                VALUE_REDIS_DOWN = "value-down";
	private              RedisReactiveCommands<String, String> commands;

	@Before
	public void setup() {
		proxy = toxiproxy.getProxy(redis, 6379);
		RedisClient client = RedisClient
			.create("redis://localhost:" + proxy.getProxyPort());
		commands = client.connect().reactive();
	}

	@Test
	public void shouldNotExecuteAfterTimeout() {
		Mono<String> redisUp = Mono.fromCallable(() -> {
			proxy.setConnectionCut(false);
			return "Establish network";
		})
			.subscribeOn(Schedulers.elastic())
			.log()
			.ignoreElement();

		Mono<String> redisDown = Mono.fromCallable(() -> {
			proxy.setConnectionCut(true);
			return "Cutting network";
		})
			.subscribeOn(Schedulers.elastic())
			.log()
			.ignoreElement();

		Mono<String> source = Flux.concat(
			commands
				.set(KEY, VALUE_REDIS_UP)
				.log()
				.ignoreElement(),

			redisDown,

			commands
				.set(KEY, VALUE_REDIS_DOWN)
				.doOnSubscribe(subscription -> log.info("SET command subscribed"))
				.timeout(Duration.ofMillis(100), Mono.defer(() -> {
					log.info("Got expected timeout");
					return Mono.empty();
				}))
				.log(),

			redisUp.delaySubscription(Duration.ofMillis(500))
		)
			.then(commands.get(KEY).log());

		StepVerifier
			.create(source)
			.expectNext(VALUE_REDIS_UP)
			.verifyComplete();
	}
}
