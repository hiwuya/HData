package io.jayer.hdata.core;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * @author Jayer
 * @date 2019-07-25
 */
public class BeamDemo {

    public static void main(String[] args) {
        JdbcIO.DataSourceConfiguration dataSourceConfiguration = JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://10.247.17.31:3306/wuya_test?useSSL=false").withUsername("51generalnew").withPassword("uUZPeL32FpatOvju");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setJobName("Beam Test");
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(Create.of((Void) null))
                .apply(ParDo.of(new JdbcReadSplittableRowDoFn()))
                .apply(JdbcIO.<Row>write()
                        .withDataSourceConfiguration(dataSourceConfiguration)
                        .withStatement("INSERT IGNORE INTO `T_Test_2` (`AutoId`, `Day`) VALUES (?, ?)")
                        .withPreparedStatementSetter((row, query) -> {
                                    query.setLong(1, row.getInt64(0));
                                    query.setString(2, row.getString(1));
                                }
                        )
                );

        pipeline.run().waitUntilFinish();
    }
}
