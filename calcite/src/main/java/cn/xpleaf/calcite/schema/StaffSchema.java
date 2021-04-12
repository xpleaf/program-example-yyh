package cn.xpleaf.calcite.schema;

public class StaffSchema {

    @Override
    public String toString() {
        return "StaffSchema";
    }

    public final User[] users = {
            new User(1, "leaf", 25, 2),
            new User(2, "xpleaf", 27, 1),
            new User(3, "yingbao", 26, 1)
    };

    public final Company[] companies = {
            new Company(1, "netease"),
            new Company(2, "huawei")
    };

    public static class User {
        public int id;
        public String name;
        public int age;
        public int company;

        public User(int id, String name, int age, int company) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.company = company;
        }
    }

    public static class Company {
        public int id;
        public String name;

        public Company(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
