package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @auther: zk
 * @date: 2021/12/8 10:48
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Keyed<IN, KEY, ID> {
    private IN wrapped;
    private KEY key;
    private ID id;
}
