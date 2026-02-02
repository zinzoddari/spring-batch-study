package kr.co.batch.csv.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Comment;

@Getter
@Entity
@Table(name = "user_info")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class User {

    @Id
    @Column(nullable = false)
    private Long id;

    @Comment("이름")
    @Column(name = "name", columnDefinition = "varchar(32)", nullable = false)
    private String name;

    @Comment("나이")
    @Column(name = "age", columnDefinition = "tinyint", nullable = false)
    private Integer age;

    public static User create(final Long id, final String name, final Integer age) {
        User user = new User();

        user.id = id;
        user.name = name;
        user.age = age;

        return user;
    }
}
