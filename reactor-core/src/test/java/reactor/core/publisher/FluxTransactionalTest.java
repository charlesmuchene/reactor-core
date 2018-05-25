/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxTransactionalTest {

	static class TestResource {

		final Level level;

		PublisherProbe<Integer> commitProbe = PublisherProbe.empty();
		PublisherProbe<Integer> rollbackProbe = PublisherProbe.empty();

		TestResource() {
			this.level = Level.INFO;
		}

		TestResource(Level level) {
			this.level = level;
		}

		public Flux<String> data() {
			return Flux.just("Transaction started");
		}

		public Flux<Integer> commit() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .log("commit", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		public Flux<Integer> commitDelay() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .delayElements(Duration.ofMillis(500))
					    .log("commit", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		public Flux<Integer> commitError() {
			this.commitProbe = PublisherProbe.of(
					Flux.just(3, 2, 1)
					    .delayElements(Duration.ofMillis(500))
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("commit", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return commitProbe.flux();
		}

		public Flux<Integer> rollback() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .log("rollback", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackDelay() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(Duration.ofMillis(500))
					    .log("rollback", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

		public Flux<Integer> rollbackError() {
			this.rollbackProbe = PublisherProbe.of(
					Flux.just(5, 4, 3, 2, 1)
					    .delayElements(Duration.ofMillis(500))
					    .map(i -> 100 / (i - 1)) //results in divide by 0
					    .log("rollback", level, SignalType.ON_NEXT, SignalType.ON_COMPLETE));
			return rollbackProbe.flux();
		}

	}

	@Test
	public void apiCommit() {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.transactional(TestResource::new,
				TestResource::commit,
				TestResource::rollback,
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction", "more work in transaction");
				});

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .expectComplete()
		            .verify();

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	public void apiCommitFailure() {
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.transactional(TestResource::new,
				TestResource::commitError,
				TestResource::rollback,
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction", "more work in transaction");
				});

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .expectNext("more work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
		            .hasMessage("Transaction commit failed")
		            .hasCauseInstanceOf(ArithmeticException.class));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> tr.commitProbe.wasSubscribed(), "commit")
				.matches(tr -> !tr.rollbackProbe.wasSubscribed(), "no rollback");
	}

	@Test
	public void apiRollback() {
		final RuntimeException rollbackCause = new IllegalStateException("boom");
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.transactional(TestResource::new,
				TestResource::commitError,
				TestResource::rollback,
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction")
							.concatWith(Mono.error(rollbackCause));
				});

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
		            .hasMessage("boom")
		            .hasNoCause()
		            .hasNoSuppressedExceptions());

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback");
	}

	@Test
	public void apiRollbackFailure() {
		final RuntimeException rollbackCause = new IllegalStateException("boom");
		final AtomicReference<TestResource> ref = new AtomicReference<>();
		Flux<String> flux = Flux.transactional(TestResource::new,
				TestResource::commitError,
				TestResource::rollbackError,
				d -> {
					ref.set(d);
					return d.data().concatWithValues("work in transaction")
							.concatWith(Mono.error(rollbackCause));
				});

		StepVerifier.create(flux)
		            .expectNext("Transaction started")
		            .expectNext("work in transaction")
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("Transaction rollback failed")
				            .hasCauseInstanceOf(ArithmeticException.class)
				            .hasSuppressedException(rollbackCause));

		assertThat(ref.get())
				.isNotNull()
				.matches(tr -> !tr.commitProbe.wasSubscribed(), "no commit")
				.matches(tr -> tr.rollbackProbe.wasSubscribed(), "rollback");
	}

}