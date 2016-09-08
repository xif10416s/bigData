package org.fxi.test.jvm.BlackMagicOfMethodDispatch;

import org.fxi.test.jvm.BlackMagicOfMethodDispatch.one.One;
import org.fxi.test.jvm.BlackMagicOfMethodDispatch.three.Three;
import org.fxi.test.jvm.BlackMagicOfMethodDispatch.two.Two;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Created by xifei on 16-8-26.
 */
public class MainTest {
	public static void main(String[] args) throws RunnerException {
//		Options opt = new OptionsBuilder().include(One.class.getSimpleName()).forks(1).build();
//		new Runner(opt).run();
		new Runner(new OptionsBuilder().include(Two.class.getSimpleName()).forks(1).build()).run();
//		new Runner(new OptionsBuilder().include(Three.class.getSimpleName()).forks(1).build()).run();
	}
}
