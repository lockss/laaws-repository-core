package org.lockss.util.test;

import java.util.*;

import org.junit.jupiter.api.extension.*;

class VariantTestInvocationContext implements TestTemplateInvocationContext {

  private final VariantTestNameFormatter formatter;
  private final String vName;

  VariantTestInvocationContext(VariantTestNameFormatter formatter,
			       String vName) {
    this.formatter = formatter;
    this.vName = vName;
  }

  @Override
  public String getDisplayName(int invocationIndex) {
    return this.formatter.format(invocationIndex, this.vName);
  }

  @Override
  public List<Extension> getAdditionalExtensions() {
    return Collections.singletonList(new BeforeEachCallback() {
	@Override
	public void beforeEach(ExtensionContext context) {
// 	  log.info("beforeEach: " + context);
// 	  log.info("testInstance: " + context.getTestInstance());
	  Optional optInst = context.getTestInstance();
	  if (optInst.isPresent()) {
	    LTC5 ltc5 = (LTC5)optInst.get();
// 	  log.info("setUpVariant: " + vName);
	    ltc5.setUpVariant(vName);
	  }
	}
      });
  }
}
