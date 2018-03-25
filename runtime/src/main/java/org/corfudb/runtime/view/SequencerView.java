package org.corfudb.runtime.view;

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.CFUtils;


/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    public TokenResponse nextConditionalToken(@Nullable TxResolutionInfo info, UUID... streams) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .getToken(new TokenRequest(true, info, streams))));
    }

    public TokenResponse nextToken(UUID... streams) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .getToken(new TokenRequest(true, null, streams))));
    }

    public TokenResponse currentToken(UUID... streams) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .getToken(new TokenRequest(false, null, streams))));
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * <p>If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.</p>
     *
     * @param streamIDs The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        if (numTokens == 0) {
            return currentToken(streamIDs.toArray(new UUID[0]));
        } else if (numTokens == 1) {
            return nextToken(streamIDs.toArray(new UUID[0]));
        }
        throw new UnrecoverableCorfuError("numTokens other than 0,1 no longer supported");
    }


    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {
        if (numTokens != 1) {
            throw new UnrecoverableCorfuError("numTokens other than 1 no longer supported");
        }

        return nextConditionalToken(conflictInfo, streamIDs.toArray(new UUID[0]));
    }

    public void trimCache(long address) {
        runtime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient().trimCache(address);
    }
}