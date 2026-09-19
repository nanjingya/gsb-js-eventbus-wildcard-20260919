# eventbus

同步事件总线。

```js
import { EventBus } from "./eventbus.js";

const bus = new EventBus();
bus.on("user.login", (u) => console.log(u));
bus.emit("user.login", { id: 1 });
```

运行测试：`node --test`
