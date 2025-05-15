import java.util.Optional;
import java.util.function.BiConsumer;

public class Acceptor {
    MonotonicId promisedGeneration = MonotonicId.empty();

    Optional<MonotonicId> acceptedGeneration = Optional.empty();
    Optional<Command> acceptedValue = Optional.empty();

    Optional<Command> committedValue = Optional.empty();
    Optional<MonotonicId> committedGeneration = Optional.empty();

    public AcceptorState state = AcceptorState.NEW;
    private BiConsumer<Acceptor, Command> kvStore;

    public PrepareResponse prepare(MonotonicId generation) {
        if (promisedGeneration.isAfter(generation)) {
            return new PrepareResponse(false, acceptedValue,
                    acceptedGeneration, committedGeneration, committedValue);
        }
        promisedGeneration = generation;
        state = AcceptorState.PROMISED;
        return new PrepareResponse(true, acceptedValue, acceptedGeneration,
                committedGeneration, committedValue);
    }

    public boolean accept(MonotonicId generation, Command value) {
        if (promisedGeneration.isBefore(generation)) {
            return false;
        }
        acceptedGeneration = Optional.of(generation);
        acceptedValue = Optional.of(value);
        state = AcceptorState.ACCEPTED;
        return true;
    }

    public void resetPaxosState() {
        // 이 구현은 커밋한 캆을 저장하지 않고 준비 단계에서 별도로 처리하지 않는다면 문제가 발생한다.
        promisedGeneration = MonotonicId.empty();
        acceptedGeneration = Optional.empty();
        acceptedValue = Optional.empty();
    }

}
