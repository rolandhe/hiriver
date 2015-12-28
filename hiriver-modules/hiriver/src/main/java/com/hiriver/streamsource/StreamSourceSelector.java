package com.hiriver.streamsource;

import java.util.List;

public interface StreamSourceSelector {
    StreamSource select(final List<StreamSource> haStreamSourceList);
}
