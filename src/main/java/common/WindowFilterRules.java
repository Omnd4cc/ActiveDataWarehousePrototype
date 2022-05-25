package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * @auther: zk
 * @date: 2021/12/22 10:29
 */
@EqualsAndHashCode
@Data
@AllArgsConstructor
public class WindowFilterRules implements Serializable {
    private String field;
    private String operator;
    private String value;
}
